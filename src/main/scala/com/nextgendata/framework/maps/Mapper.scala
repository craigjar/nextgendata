package com.nextgendata.framework.maps

/**
  * Created by Craig on 2016-04-26.
  *
  * This is the generic trait (or interface) for all ref & map lookup objects.  To make the Mapper
  * objects more natural and flexible it extends the built-in Scala immutable Map interface and
  * adds additional functions required to provide a convenient ref & map API.
  *
  * This is the base class for all mappers which allows for generic stackable traits to be mixed in
  * or decorate mapper implementations is a reusable and composable way.
  */
trait Mapper[K, V] extends Map[K, V] with Serializable {

  /**
    * Mappers must provide getDefault implementation which returns a default value record.
    *
    * This is useful when a source value which is being mapped is not provided and the application
    * must use defaulted mapping values.
    *
    * @return
    */
  def getDefault: V

  /**
    * Mappers must provide a getInvalid implementation which returns an invalid value record.
    *
    * This is useful when a source value which is being mapped does not find a matching value
    * in the lookup and the application needs to treat this as an invalid mapping values.
    *
    * @return
    */
  def getInvalid: V

  /**
    * As the key value is a generic Type K, this function assist with generic code so that
    * stackable traits or decorators can be mixed in and match provided source keys in the
    * lookups with an "empty" value and then provide default return values instead for example.
    *
    * @return
    */
  def getEmptyKey: K
}
