package latest

import java.io.IOException
import java.net.URLConnection
import scala.collection.SortedMap
import sttp.tapir.ztapir.*
import sttp.tapir.server.ziohttp.ZioHttpInterpreter
import sttp.tapir.CodecFormat
import sttp.capabilities.zio.ZioStreams
import sttp.capabilities.WebSockets
import zio.*
import zio.stream.*
import sttp.model.StatusCode
import sttp.tapir.server.ziohttp.ZioHttpServerOptions
import sttp.tapir.server.interceptor.log.DefaultServerLog
import zio.http.Server as ZServer
import scala.util.Using

val LoggingApi =
  // optionally do some configuring of
  // your logging library here
  import java.util.logging.*
  val configPropsStr = "java.util.logging.SimpleFormatter.format=%1$tF@%1$tT [%4$s] [%3$s] %5$s %6$s%n\n"
  LogManager.getLogManager().readConfiguration( new ByteArrayInputStream( configPropsStr.getBytes(StandardCharsets.UTF_8) ) )
  val ch = new ConsoleHandler()
  ch.setFormatter( new SimpleFormatter() ) 
  Logger.getLogger("").addHandler(ch);
  Logger.getLogger("").setLevel(Level.FINEST);
  logadapter.zio.ZApi( logadapter.jul.Api )

import LoggingApi.*
import scala.util.control.NonFatal
import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

given LogAdapter = logAdapterFor("latest")

abstract class LatestException( message : String, cause : Throwable = null ) extends Exception( message, cause )
final case class NotAFile( message : String, cause : Throwable = null ) extends LatestException( message, cause )
final case class NotFound( message : String, cause : Throwable = null ) extends LatestException( message, cause )
final case class NoSuchDirectories( message : String, cause : Throwable = null ) extends LatestException( message, cause )

object Default:
  val chooseLatest : IndexedSeq[os.Path] => Option[os.Path] = paths => // last by modified time
    paths
      .filter( _.lastOpt.fold(false)(lastSegment => !lastSegment.startsWith(".")) ) // always ignore dotfiles
      .map( path => (path, os.stat(path) ) )
      .filter( _(1).isFile )
      .map( tup => (tup(1).mtime, tup(0)) )
      .to(SortedMap)
      .lastOption
      .map( _(1) )

  val metadata : os.Path => ( downloadFileName : String, contentType : String ) = path =>
    val fname = path.lastOpt.getOrElse: 
      throw new NotAFile( s"The latest file chosen is apparently the root directory, no file name? $path" )
    val ct = Option(URLConnection.guessContentTypeFromName(fname)).getOrElse("application/octet-stream")
    ( fname, ct )

  val filter : os.Path => Boolean = _ => true

private val K32 = 32 * 1024

private val errorHandler =
  oneOf[Throwable](
    oneOfVariant(statusCode(StatusCode.NotFound).and( stringBody.map(msg => NotFound(msg))(_.getMessage) )),
    oneOfVariant(statusCode(StatusCode.InternalServerError).and( stringBody.map(msg => Throwable(msg))(_.getMessage) ))
  )

private val BaseEndpoint = endpoint.errorOut( errorHandler )

case class Config(
  dirPath : os.Path,
  filter : os.Path => Boolean = Default.filter,
  chooseLatest : IndexedSeq[os.Path] => Option[os.Path] = Default.chooseLatest,
  metadata : os.Path => ( downloadFileName : String, contentType : String ) = Default.metadata,
  chunkSize : Int = K32
)

private def tapirEndpoint( path : List[String], config : Config ) : Option[ZServerEndpoint[Any,ZioStreams & WebSockets]] =
  config.chooseLatest( os.list( config.dirPath ) ).map: latest =>
    val ( fname, ct ) = config.metadata(latest)
    val definition =
      path
        .foldLeft( BaseEndpoint )( (accum,next) => accum.in( next ) )
        .out( header("Content-Type", ct) )
        .out( header("Content-Disposition", s"""attachment; filename="$fname"""") )
        .out( streamBinaryBody(ZioStreams)(CodecFormat.OctetStream()) )
    val load : Unit => Task[ZStream[Any,IOException,Byte]] = 
      unit =>
        ZIO.attemptBlocking(
          config.chooseLatest( os.list( config.dirPath ) ) match
            case Some( latest ) => ZStream.fromInputStream( os.read.inputStream( latest ), config.chunkSize )
            case None => throw new NotFound( s"No latest regular file could be located in directory ${config.dirPath}" )
        )
    definition.zServerLogic( load ).widen[Any]

def serveCurrent( port : Int, pathsToConfigs : Map[List[String],Config], verbose : Boolean= false ) : Task[Unit] =
  val VerboseServerInterpreterOptions: ZioHttpServerOptions[Any] =
  // modified from https://github.com/longliveenduro/zio-geolocation-tapir-tapir-starter/blob/b79c88b9b1c44a60d7c547d04ca22f12f420d21d/src/main/scala/com/tsystems/toil/Main.scala
    ZioHttpServerOptions
      .customiseInterceptors
      .serverLog(
        DefaultServerLog[Task](
          doLogWhenReceived = msg => INFO.zlog(msg),
          doLogWhenHandled = (msg, error) => error.fold(INFO.zlog(msg))(err => WARNING.zlog(s"msg: ${msg}, err: ${err}")),
          doLogAllDecodeFailures = (msg, error) => error.fold(INFO.zlog(msg))(err => WARNING.zlog(s"msg: ${msg}, err: ${err}")),
          doLogExceptions = (msg: String, exc: Throwable) => WARNING.zlog(msg, exc),
          noLog = ZIO.unit
        )
      )
      .options
  val DefaltServerInterpreterOptions: ZioHttpServerOptions[Any] = ZioHttpServerOptions.default.widen[Any]
  def interpreterOptions( verbose : Boolean ) = if verbose then VerboseServerInterpreterOptions else DefaltServerInterpreterOptions
  for
    _            <- INFO.zlog( s"Serving configured latest files on port $port" )
    _            <- ZIO.collectAllDiscard( pathsToConfigs.map( (p,c) => INFO.zlog( "    " + p.mkString("/") + " --> " + c.dirPath ) ) )
    endpoints     = pathsToConfigs.map( (p,c) => tapirEndpoint(p,c) ).flatten.toList
    serverConfig  = ZServer.Config.default.port(port).requestStreaming(ZServer.RequestStreaming.Enabled)
    httpApp       = ZioHttpInterpreter(interpreterOptions(verbose)).toHttp(endpoints)
    _            <- ZServer
                      .serve(httpApp)
                      .provide(ZLayer.succeed(serverConfig), ZServer.live)
                      .zlogErrorDefect( ERROR, "Something went wrong while serving configured latest files:" )
  yield ()

private def watchForChanges( dirs : Seq[os.Path], fiber : Fiber[Throwable,Unit], promise : Promise[Throwable,Unit] ) : Task[Unit] = //ZIO.never
  def onChanged( paths : Set[os.Path] ) : Task[Unit] =
    //println("onChanged(...)")
    for
      _ <- INFO.zlog( s"paths changed: $paths -- recreating server!" )
      _ <- fiber.interrupt
    yield ()
  ZIO.asyncZIO: callback =>
    //println( s"dirs: $dirs" )
    val acquire = ZIO.attempt( os.watch.watch( dirs, paths => callback(onChanged(paths)) ) )
    val release : AutoCloseable => URIO[Any,Unit] = ac =>
       ZIO.succeed( ac.close() ).zlogErrorDefect(WARNING)
    def use : AutoCloseable => Task[Unit] = _ =>
        //println("Awaiting the promise.")
        promise.await
    ZIO.acquireReleaseWith( acquire )( release )( use )

private val RetrySchedule =
  Schedule.fixed( 30.seconds ) && Schedule.recurWhile[Throwable]:
      case _ : NoSuchDirectories => // don't retry on misconfiguration
        false
      case NonFatal(_) => // in general retry
        true
      case _ => // but don't retry fatal throwables
        false

def serveTask( port : Int, pathsToConfigs : Map[List[String],Config], verbose : Boolean= false ) : Task[Unit] =
  val dirs = pathsToConfigs.map( _(1).dirPath ).toSeq

  val nonexistent = dirs.filter( dir => !os.exists( dir ) )
  if nonexistent.nonEmpty then
    throw new NoSuchDirectories( "The following directories are configured but do not exist: " + nonexistent.mkString(", ") )

  val nondirectory = dirs.filter( dir => !os.isDir( dir ) )
  if nonexistent.nonEmpty then
    throw new NoSuchDirectories( "The following directories are configured, exist in the file system, but are not directories: " + nondirectory.mkString(", ") )

  def serverTask( promise : Promise[Throwable,Unit] ) =
    serveCurrent( port, pathsToConfigs, verbose )
      .retry( RetrySchedule )
      .onInterrupt:
         INFO.zlog("Interrupt detected. Shutting down server, will recreate.")
         promise.succeed( () )

  for
    p       <- Promise.make[Throwable,Unit]
    serverf <- serverTask( p ).fork
    _       <- watchForChanges( dirs, serverf, p )
    _       <- serveTask( port, pathsToConfigs, verbose )
  yield()


def serve( port : Int, pathsToConfigs : Map[List[String],Config], verbose : Boolean= false ) : Unit =
  Unsafe.unsafely:
    Runtime.default.unsafe.run(serveTask(port,pathsToConfigs,verbose).zlogErrorDefect(ERROR)).getOrThrow()

