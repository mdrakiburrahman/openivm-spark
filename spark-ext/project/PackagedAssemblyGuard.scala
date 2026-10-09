import java.io.{BufferedInputStream, BufferedOutputStream, ByteArrayOutputStream, File, FileOutputStream, InputStream}
import java.net.URLClassLoader
import java.util.jar.{JarEntry, JarFile, JarOutputStream}

import sbt.IO

final class PackagedAssemblyValidationException(message: String, cause: Throwable = null)
    extends RuntimeException(message, cause)

object PackagedAssemblyGuard {
  val LexerClassName = "org.openivm.spark.parser.gen.IvmSqlBaseLexer"
  val LexerEntryName = LexerClassName.replace('.', '/') + ".class"

  def verify(assembly: File): Unit = {
    val canonicalAssembly = assembly.getCanonicalFile
    if (!canonicalAssembly.isFile)
      throw new PackagedAssemblyValidationException(s"Assembly does not exist: $canonicalAssembly")

    val packaged = new JarFile(canonicalAssembly)
    try {
      if (packaged.getJarEntry(LexerEntryName) == null)
        throw new PackagedAssemblyValidationException(
          s"Assembly $canonicalAssembly does not contain $LexerEntryName"
        )
    } finally packaged.close()

    val loader                = new URLClassLoader(Array(canonicalAssembly.toURI.toURL), null)
    val thread                = Thread.currentThread()
    val previousContextLoader = thread.getContextClassLoader
    try {
      thread.setContextClassLoader(loader)
      val lexer = Class.forName(LexerClassName, true, loader)
      val loadedFrom = Option(lexer.getProtectionDomain)
        .flatMap(domain => Option(domain.getCodeSource))
        .flatMap(source => Option(source.getLocation))
        .map(url => new File(url.toURI).getCanonicalFile)
      if (!loadedFrom.contains(canonicalAssembly))
        throw new PackagedAssemblyValidationException(
          s"$LexerClassName loaded from ${loadedFrom.getOrElse("<unknown>")} instead of $canonicalAssembly"
        )
    } catch {
      case error: PackagedAssemblyValidationException => throw error
      case error: Throwable =>
        throw new PackagedAssemblyValidationException(
          s"Packaged lexer initialization failed for $canonicalAssembly: ${causeSummary(error)}",
          error
        )
    } finally {
      thread.setContextClassLoader(previousContextLoader)
      loader.close()
    }
  }

  def causeChain(error: Throwable): Seq[Throwable] = {
    val causes             = Seq.newBuilder[Throwable]
    var current: Throwable = error
    while (current != null) {
      causes += current
      current = current.getCause
    }
    causes.result()
  }

  private def causeSummary(error: Throwable): String =
    causeChain(error)
      .map { cause =>
        val message = Option(cause.getMessage).filter(_.nonEmpty).map(value => s": $value").getOrElse("")
        s"${cause.getClass.getSimpleName}$message"
      }
      .mkString(" -> ")
}

object PackagedAssemblyGuardRegression {
  private val ShadedAntlrPrefix = "org/openivm/shaded/antlr/"

  def verifyRejectsIncompatibleLexer(assembly: File, workDirectory: File): Unit = {
    IO.createDirectory(workDirectory)
    val incompatibleAssembly = new File(workDirectory, "incompatible-antlr-assembly.jar")
    writeIncompatibleAssembly(assembly, incompatibleAssembly)
    try {
      PackagedAssemblyGuard.verify(incompatibleAssembly)
      throw new IllegalStateException(
        s"Packaged assembly guard accepted the deliberately incompatible lexer in $incompatibleAssembly"
      )
    } catch {
      case error: PackagedAssemblyValidationException
          if PackagedAssemblyGuard
            .causeChain(error)
            .exists(_.isInstanceOf[java.io.InvalidClassException]) =>
        ()
      case error: PackagedAssemblyValidationException =>
        throw new IllegalStateException(
          "Packaged assembly guard rejected the fixture for an unexpected reason",
          error
        )
    } finally IO.delete(incompatibleAssembly)
  }

  private def writeIncompatibleAssembly(source: File, destination: File): Unit = {
    val input                = new JarFile(source)
    val output               = new JarOutputStream(new BufferedOutputStream(new FileOutputStream(destination)))
    var copiedLexer          = false
    var copiedRuntimeClasses = 0
    try {
      val entries = input.entries()
      while (entries.hasMoreElements) {
        val entry = entries.nextElement()
        val name  = entry.getName
        if (
          !entry.isDirectory && (name == PackagedAssemblyGuard.LexerEntryName || name.startsWith(
            ShadedAntlrPrefix
          ))
        ) {
          val bytes = readAll(input.getInputStream(entry))
          val packagedBytes =
            if (name == PackagedAssemblyGuard.LexerEntryName) {
              copiedLexer = true
              corruptSerializedAtnVersion(bytes)
            } else {
              copiedRuntimeClasses += 1
              bytes
            }
          val copiedEntry = new JarEntry(name)
          if (entry.getTime >= 0) copiedEntry.setTime(entry.getTime)
          output.putNextEntry(copiedEntry)
          output.write(packagedBytes)
          output.closeEntry()
        }
      }
    } finally {
      output.close()
      input.close()
    }

    if (!copiedLexer || copiedRuntimeClasses == 0)
      throw new IllegalStateException(
        s"Could not create incompatible fixture from $source (lexer=$copiedLexer, runtimeClasses=$copiedRuntimeClasses)"
      )
  }

  private def corruptSerializedAtnVersion(classBytes: Array[Byte]): Array[Byte] = {
    if (readU4(classBytes, 0) != 0xcafebabeL)
      throw new IllegalArgumentException("Generated lexer is not a Java class file")

    val candidates        = Seq.newBuilder[(Int, Int)]
    val constantPoolCount = readU2(classBytes, 8)
    var constantPoolIndex = 1
    var offset            = 10
    while (constantPoolIndex < constantPoolCount) {
      val tag = readU1(classBytes, offset)
      offset += 1
      tag match {
        case 1 =>
          val length     = readU2(classBytes, offset)
          val dataOffset = offset + 2
          if (length > 1000 && (readU1(classBytes, dataOffset) == 3 || readU1(classBytes, dataOffset) == 4))
            candidates += dataOffset -> length
          offset = dataOffset + length
        case 3 | 4 | 9 | 10 | 11 | 12 | 17 | 18 => offset += 4
        case 5 | 6 =>
          offset += 8
          constantPoolIndex += 1
        case 7 | 8 | 16 | 19 | 20 => offset += 2
        case 15                   => offset += 3
        case unsupported =>
          throw new IllegalArgumentException(s"Unsupported class-file constant-pool tag $unsupported")
      }
      constantPoolIndex += 1
    }

    val serializedAtn = candidates.result().sortBy(-_._2).headOption.getOrElse {
      throw new IllegalArgumentException("Could not find the generated lexer's serialized ATN constant")
    }
    val corrupted      = classBytes.clone()
    val currentVersion = readU1(corrupted, serializedAtn._1)
    corrupted(serializedAtn._1) = (if (currentVersion == 3) 4 else 3).toByte
    corrupted
  }

  private def readAll(input: InputStream): Array[Byte] = {
    val buffered = new BufferedInputStream(input)
    val output   = new ByteArrayOutputStream()
    val buffer   = new Array[Byte](8192)
    try {
      var read = buffered.read(buffer)
      while (read >= 0) {
        if (read > 0) output.write(buffer, 0, read)
        read = buffered.read(buffer)
      }
      output.toByteArray
    } finally buffered.close()
  }

  private def readU1(bytes: Array[Byte], offset: Int): Int =
    bytes(offset) & 0xff

  private def readU2(bytes: Array[Byte], offset: Int): Int =
    (readU1(bytes, offset) << 8) | readU1(bytes, offset + 1)

  private def readU4(bytes: Array[Byte], offset: Int): Long =
    (readU1(bytes, offset).toLong << 24) |
      (readU1(bytes, offset + 1).toLong << 16) |
      (readU1(bytes, offset + 2).toLong << 8) |
      readU1(bytes, offset + 3).toLong
}
