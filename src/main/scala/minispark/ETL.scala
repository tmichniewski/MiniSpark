package com.github
package minispark

/**
 * Represents a parameterless function with no return value.
 * It is a complete ETL process.
 * Once triggered, it extracts, transforms and loads.
 */
trait ETL extends (() => Unit)
