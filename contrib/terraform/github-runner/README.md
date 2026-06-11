# GitHub self-hosted runner on Azure

Thin wrapper around [`KangarooKube/terraform-infrastructure-modules//modules/github-runner/azure-vmss`](https://github.com/KangarooKube/terraform-infrastructure-modules/tree/main/modules/github-runner/azure-vmss).

```bash
contrib/deploy-gh-runner.sh apply     # ~15 min end-to-end
contrib/deploy-gh-runner.sh destroy
contrib/deploy-gh-runner.sh plan
```

Requires `gh auth status` logged in (fresh runner token is minted per apply).

The runner labels itself `openivm-spark-azure`; CI in `.github/workflows/gci.yaml` targets that via `runs-on: [self-hosted, openivm-spark-azure]`.

## Override VMSS sizing

Set in `.env`:

```bash
TF_VAR_location=australiacentral
TF_VAR_instance_sku=Standard_E48as_v5
TF_VAR_instance_count=2
```

## SSH

```bash
RG=$(grep ^TF_RESOURCE_GROUP .env | cut -d= -f2)
INSTANCE_ID=$(az vmss list-instances -g "$RG" -n openivm-spark-vmss-runner --query '[0].id' -o tsv)
az network bastion ssh -g "$RG" -n openivm-spark-bas \
  --target-resource-id "$INSTANCE_ID" \
  --auth-type ssh-key --username azureuser --ssh-key ~/.ssh/id_ed25519
```

Or read `ssh_via_bastion_hint` / `tunnel_via_bastion_hint` from `terraform apply` output.

## Bump pinned module

Edit `?ref=<SHA>` in `main.tf`, then `deploy-gh-runner.sh plan`.
