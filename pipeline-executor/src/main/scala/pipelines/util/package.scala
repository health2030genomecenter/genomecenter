package org.gc.pipelines

package object util {
  def isMac = System.getProperty("os.name").toLowerCase.contains("mac")
  def isLinux = System.getProperty("os.name").toLowerCase.contains("linux")
}
