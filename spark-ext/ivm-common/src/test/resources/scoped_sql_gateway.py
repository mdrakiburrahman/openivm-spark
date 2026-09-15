"""Synthetic pooled-Py4J regression, launched only by ScopedSqlBridgeSpec."""
import concurrent.futures
import json
import sys

from py4j.java_gateway import GatewayClient, GatewayParameters, JavaGateway
from py4j.protocol import Py4JJavaError


client = GatewayClient(address="127.0.0.1", port=int(sys.argv[1]))
gateway = JavaGateway(
    gateway_client=client,
    gateway_parameters=GatewayParameters(port=int(sys.argv[1]), auto_convert=True),
)
fixture = gateway.entry_point
assert type(gateway._gateway_client) is GatewayClient
control = JavaGateway(gateway_parameters=GatewayParameters(port=int(sys.argv[1])))
try:
    if sys.argv[2:] == ["disconnect"]:
        gateway.jvm.org.openivm.spark.common.ScopedSqlBridge.execute(
            fixture.spark(), "SCOPED FIXTURE disconnected ok", "disconnected",
            "group-disconnected", "description-disconnected", "pool-disconnected", True
        )
        raise AssertionError("test process should have been terminated before the SQL completed")

    # A normal Python thread is not pinned to its first gateway connection.
    # A separate control client observes latches without taking a pooled data connection.
    original_thread = fixture.legacySet("request-a")
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        held = executor.submit(fixture.holdConnection)
        control.entry_point.awaitBlocked()
        seen_thread, seen_request = fixture.legacyRead().split(":")
        assert original_thread != int(seen_thread)
        assert seen_request == "null"  # the same Python request has lost its JVM-local request ID
        control.entry_point.releaseConnection()
        held.result(timeout=45)

    # All 32 driver-side commands must overlap before any may finish.
    fixture.prepare(32)
    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
        futures = [
            executor.submit(fixture.executeAndVerifyRestore, "pooled-%d" % index, index == 7)
            for index in range(32)
        ]
        fixture.awaitOverlap()
        fixture.releaseOverlap()
        for index, future in enumerate(futures):
            if index == 7:
                try:
                    future.result(timeout=90)
                    raise AssertionError("SQL failure was swallowed")
                except Py4JJavaError as error:
                    assert "synthetic SQL failure" in str(error)
            else:
                future.result(timeout=90)
            state = json.loads(fixture.snapshot("pooled-%d" % index))
            assert state["sql_succeeded"] is (index != 7)
            assert len(state["invocations"]) == 1
            assert state["invocations"][0]["refresh_id"] == "native-pooled-%d" % index
            assert state["pending_flushes"] == 0
            if index != 7:
                assert state["capture_complete"] is True

    # Reuse a pooled server thread without inheriting the previous request.
    fixture.prepare(0)
    fixture.releaseOverlap()
    fixture.executeAndVerifyRestore("reused", False)
    assert json.loads(fixture.snapshot("reused"))["sql_succeeded"] is True

    # The public static bridge is itself callable, with no Python begin/end RPCs.
    bridge = gateway.jvm.org.openivm.spark.common.ScopedSqlBridge
    assert bridge.apiVersion() == 1
    assert bridge.execute(
        fixture.spark(), "SCOPED FIXTURE direct ok", "direct",
        "group-direct", "description-direct", "pool-direct", True
    ) is None
    assert json.loads(fixture.snapshot("direct"))["capture_complete"] is True
finally:
    gateway.close()
    control.close()
