package org.saled
package data.builder

trait Builder[E] {
  def build(): E
}
