package org.gc.pipelines.util
import com.typesafe.config.{Config => TypesafeConfig}

object Config {
  def option[T](config: TypesafeConfig, path: String)(
      extract: TypesafeConfig => String => T): Option[T] =
    if (config.hasPath(path)) Some(extract(config)(path))
    else None
}
