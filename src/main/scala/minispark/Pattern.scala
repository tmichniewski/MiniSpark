package com.github
package minispark

import minispark.Functions.map

import org.apache.spark.sql.Encoder

/**
 * "At the beginning there was a map function, but then a few extensions came."
 *
 * This object contains a few additional versions of the map function.
 * Generally we say that if you provide the types and the mapper function,
 * then you will get the map function in return. This is the contract of those patterns.
 */
object Pattern {
  /**
   * The simplest pattern, which basically is a direct extension of the map function,
   * except that it may work on subtype of Input type.
   *
   * @tparam Input Type of input data.
   * @tparam Output Type of output data.
   * @tparam Params Type of parameters.
   */
  trait SimplePattern[Input, Output, Params] {
    /**
     * Higher order method which returns the mapping function to convert input type into output type.
     *
     * @param params Parameters to construct the resulting mapping function.
     * @return Returns the mapping function to convert input into output.
     */
    def build(params: Params): Input => Output

    /**
     * Factory method which produces the map function.
     *
     * @param params Parameters to construct the resulting mapping function.
     * @param e Encoder to produce Output.
     * @tparam T Type of input data. Subtype of Input.
     * @return Returns the map function.
     */
    def apply[T <: Input](params: Params)(implicit e: Encoder[Output]): Function[T, Output] = {
      map[T, Output] {
        val mapper: Input => Output = build(params)
        (r: T) => mapper(r)
      }
    }
  }

  /**
   * The extension of the simple pattern,
   * where instead of returning just an output we return a tuple with real input and output.
   *
   * @tparam Input Type of input data.
   * @tparam Output Type of output data.
   * @tparam Params Type of parameters.
   */
  trait TuplePattern[Input, Output, Params] {
    /**
     * Higher order method which returns the mapping function to convert input type into output type.
     *
     * @param params Parameters to construct the resulting mapping function.
     * @return Returns the mapping function to convert input into output.
     */
    def build(params: Params): Input => Output

    /**
     * Factory method which produces the map function.
     *
     * @param params Parameters to construct the resulting mapping function.
     * @param e Encoder to produce tuple of T and Output.
     * @tparam T Type of input data. Subtype of Input.
     * @return Returns the map function.
     */
    def apply[T <: Input](params: Params)(implicit e: Encoder[(T, Output)]): Function[T, (T, Output)] = {
      map[T, (T, Output)] {
        val mapper: Input => Output = build(params)
        (r: T) => (r, mapper(r))
      }
    }
  }

  /**
   * The extension of the tuple pattern,
   * where instead of returning a tuple we return an object which is produced by the constructor function.
   *
   * @tparam Input Type of input data.
   * @tparam Output Type of output data.
   * @tparam Params Type of parameters.
   */
  trait ConstructorPattern[Input, Output, Params] {
    /**
     * Higher order method which returns the mapping function to convert input type into output type.
     *
     * @param params Parameters to construct the resulting mapping function.
     * @return Returns the mapping function to convert input into output.
     */
    def build(params: Params): Input => Output

    /**
     * Factory method which produces the map function.
     *
     * @param params Parameters to construct the resulting mapping function.
     * @param constructor Function to convert T and Output into U.
     * @tparam T Type of input data. Subtype of Input.
     * @tparam U Type of output data. Subtype of Output.
     * @return Returns the map function.
     */
    def apply[T <: Input, U <: Output : Encoder](params: Params)(implicit constructor: (T, Output) => U)
    : Function[T, U] = {
      map[T, U] {
        val mapper: Input => Output = build(params)
        (r: T) => constructor(r, mapper(r))
      }
    }
  }

  /**
   * The final and the most descriptive pattern.
   * Functionally it is equivalent to Constructor pattern.
   * The only difference is that Input, Output and Params types are defined inside the pattern object.
   *
   * BTW - It is recommend to define both:
   * two traits InputType and OutputType to define Input and Output types, as well as
   * two container case classes InputValue and OutputValue to store the values of input and output
   * which should extend InputType and OutputType respectively.
   * Additionally, it is recommended to define container case class to store parameters
   * which should be of Params type.
   */
  trait EnterprisePattern {
    /** Type of input data. */
    type Input
    /** Type of output data. */
    type Output
    /** Type of parameters. */
    type Params

    /**
     * Higher order method which returns the mapping function to convert input type into output type.
     *
     * @param params Parameters to construct the resulting mapping function.
     * @return Returns the mapping function to convert input into output.
     */
    def build(params: Params): Input => Output

    /**
     * Factory method which produces the map function.
     *
     * @param params Parameters to construct the resulting mapping function.
     * @param constructor Function to convert T and Output into U.
     * @tparam T Type of input data. Subtype of Input.
     * @tparam U Type of output data. Subtype of Output.
     * @return Returns the map function.
     */
    def apply[T <: Input, U <: Output : Encoder](params: Params)(implicit constructor: (T, Output) => U)
    : Function[T, U] = {
      map[T, U] {
        val mapper: Input => Output = build(params)
        (r: T) => constructor(r, mapper(r))
      }
    }
  }

  /**
   * Yet another pattern. Something as a mixture of Simple, Tuple and Constructor patterns
   * in the shape of EnterprisePattern. Its elasticity is expressed in that way,
   * that it expects to get a constructor which might produce result U,
   * which no longer has to be a subtype of Output. Instead:
   * it might be of type Output as in SimplePattern, or
   * it might be of (T, Output) tuple type as in TuplePattern, or finally
   * it might be of type U, which might be a subtype of Output as in ConstructorPattern or EnterprisePattern, or
   * it event might not be a subtype of Output which is something additional.
   *
   * BTW - It is recommend to define both:
   * two traits InputType and OutputType to define Input and Output types, as well as
   * two container case classes InputValue and OutputValue to store the values of input and output
   * which should extend InputType and OutputType respectively.
   * Additionally, it is recommended to define container case class to store parameters
   * which should be of Params type.
   */
  trait ElasticPattern {
    /** Type of input data. */
    type Input
    /** Type of output data. */
    type Output
    /** Type of parameters. */
    type Params

    /**
     * Higher order method which returns the mapping function to convert input type into output type.
     *
     * @param params Parameters to construct the resulting mapping function.
     * @return Returns the mapping function to convert input into output.
     */
    def build(params: Params): Input => Output

    /**
     * Factory method which produces the map function. This is SimplePattern like usage.
     *
     * @param params Parameters to construct the resulting mapping function.
     * @param e Encoder to produce Output.
     * @param di Dummy implicit to distinguish this apply from the next one.
     * @tparam T Type of input data. Subtype of Input.
     * @return Returns the map function.
     */
    def apply[T <: Input](params: Params)(implicit e: Encoder[Output], di: DummyImplicit): Function[T, Output] = {
      map[T, Output] {
        val mapper: Input => Output = build(params)
        (r: T) => mapper(r)
      }
    }

    /**
     * Factory method which produces the map function. This is TuplePattern like usage.
     *
     * @param params Parameters to construct the resulting mapping function.
     * @param e Encoder to produce tuple of T and Output.
     * @tparam T Type of input data. Subtype of Input.
     * @return Returns the map function.
     */
    def apply[T <: Input](params: Params)(implicit e: Encoder[(T, Output)]): Function[T, (T, Output)] = {
      map[T, (T, Output)] {
        val mapper: Input => Output = build(params)
        (r: T) => (r, mapper(r))
      }
    }

    /**
     * Factory method which produces the map function. This is ConstructorPattern and EnterprisePattern like usage.
     *
     * @param params Parameters to construct the resulting mapping function.
     * @param constructor Function to convert T and Output into U.
     * @param di Dummy implicit to distinguish this apply from the next one.
     * @tparam T Type of input data. Subtype of Input.
     * @tparam U Type of output data. Subtype of Output.
     * @return Returns the map function.
     */
    def apply[T <: Input, U <: Output : Encoder](params: Params)(implicit constructor: (T, Output) => U, di: DummyImplicit)
    : Function[T, U] = {
      map[T, U] {
        val mapper: Input => Output = build(params)
        (r: T) => constructor(r, mapper(r))
      }
    }

    /**
     * Factory method which produces the map function. This is the most general like usage.
     *
     * @param params Parameters to construct the resulting mapping function.
     * @param constructor Function to convert T and Output into U, which does not have to be subtype of Output.
     * @tparam T Type of input data. Subtype of Input.
     * @tparam U Type of output data.
     * @return Returns the map function.
     */
    def apply[T <: Input, U: Encoder](params: Params)(implicit constructor: (T, Output) => U): Function[T, U] = {
      map[T, U] {
        val mapper: Input => Output = build(params)
        (r: T) => constructor(r, mapper(r))
      }
    }
  }

  /**
   * Next pattern - the most generic. It uses the concept of getter which converts T into Input.
   * So, in general, we replace inheritance relation of T being subtype of Input
   * into necessity to provide more general higher order function called getter.
   * This way we no longer have to extend input type T with the type of Input.
   *
   * It is recommend to define Input and Output as a concrete types or containers for data.
   * We no longer need additional types for them, as it is the getter to provide Input value.
   * This way we do not need to extend T type with type of Input, as this relation is hidden inside the getter.
   * There is also no need to extend U with type of Output, as this relation is hidden inside the constructor.
   */
  trait GenericPattern {
    /** Type of input data. */
    type Input
    /** Type of output data. */
    type Output
    /** Type of parameters. */
    type Params

    /**
     * Higher order method which returns the mapping function to convert input type into output type.
     *
     * @param params Parameters to construct the resulting mapping function.
     * @return Returns the mapping function to convert input into output.
     */
    def build(params: Params): Input => Output

    /**
     * Factory method which produces the map function.
     *
     * @param params Parameters to construct the resulting mapping function.
     * @param getter Function to convert T into Input.
     * @param constructor Function to convert T and Output into U.
     * @tparam T Type of input data.
     * @tparam U Type of output data.
     * @return Returns the map function.
     */
    def apply[T, U: Encoder](params: Params, getter: T => Input, constructor: (T, Output) => U): Function[T, U] = {
      map[T, U] {
        val mapper: Input => Output = build(params)
        (r: T) => constructor(r, mapper(getter(r)))
      }
    }
  }

  /**
   * Next pattern. This is simplified elastic pattern where we require that T extends Input
   * and use only the last version of apply, with explicit constructor.
   *
   * It is recommend to define both:
   * two traits InputType and OutputType to define Input and Output types, as well as
   * two container case classes InputValue and OutputValue to store the values of input and output
   * which should extend InputType and OutputType respectively.
   * Additionally, it is recommended to define container case class to store parameters
   * which should be of Params type.
   */
  trait ThePattern {
    /** Type of input data. */
    type Input
    /** Type of output data. */
    type Output
    /** Type of parameters. */
    type Params

    /**
     * Higher order method which returns the mapping function to convert input type into output type.
     *
     * @param params Parameters to construct the resulting mapping function.
     * @return Returns the mapping function to convert input into output.
     */
    def build(params: Params): Input => Output

    /**
     * Factory method which produces the map function. This is the most general like usage.
     *
     * @param params Parameters to construct the resulting mapping function.
     * @param constructor Function to convert T and Output into U, which does not have to be subtype of Output.
     * @tparam T Type of input data. Subtype of Input.
     * @tparam U Type of output data.
     * @return Returns the map function.
     */
    def apply[T <: Input, U: Encoder](params: Params)(implicit constructor: (T, Output) => U): Function[T, U] = {
      map[T, U] {
        val mapper: Input => Output = build(params)
        (r: T) => constructor(r, mapper(r))
      }
    }
  }
}
