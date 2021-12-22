package com.github
package minispark

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.Base64

/** Serialization functions. */
object Serialize {
  /**
   * Serializes and encodes an object.
   *
   * @param o Object to be serialized.
   * @return Returns serialized object.
   */
  def serialize(o: Any): String = {
    val baos: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos: ObjectOutputStream = new ObjectOutputStream(baos)
    oos.writeObject(o)
    oos.close()
    Base64.getEncoder.encodeToString(baos.toByteArray)
  }

  /**
   * Decodes and deserializes an object.
   *
   * @param s String containing serialized object.
   * @return Returns deserialized object.
   */
  @SuppressWarnings(Array("MethodReturningAny")) // consequence of Object returned from Java deserialization
  def deserialize(s: String): AnyRef = {
    val data: Array[Byte] = Base64.getDecoder.decode(s)
    val ois: ObjectInputStream = new ObjectInputStream(new ByteArrayInputStream(data))
    val o: AnyRef = ois.readObject()
    ois.close()
    o
  }
}
