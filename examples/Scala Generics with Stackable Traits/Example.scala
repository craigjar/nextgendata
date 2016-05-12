/**
  * Created by Craig on 2016-05-12.
  */

trait Base[K, V] {
  def get(key: K): V
}

class Child1[K, V] extends Base[Int, String] {
  override def get(key: Int): String = {
    if (key == 0) "false"
    else "true"
  }
}

class Child2[K, V] extends Base[String, Int] {
  override def get(key: String): Int = {
    if (key == "false") 0
    else 1
  }
}

trait Logging[K, V] extends Base[K, V] {
  abstract override def get(key: K): V = {
    val ret = super.get(key)

    println("Key: " + key + "; Value: " + ret)

    ret
  }
}

val child1 = new Child1
child1.get(1)

val child1WithLogging = new Child1 with Logging[Int, String]
child1WithLogging.get(1)
