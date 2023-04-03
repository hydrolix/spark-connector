package io.hydrolix.spark
package pio

import spire.math.{UByte, ULong, UShort}

case class HdxManifestHeader(signature: Int,        //  4   4
                               version: Int,        //  4   8
                                  rows: ULong,      //  8  16
                               columns: ULong,      //  8  24
                          minTimestamp: ULong,      //  8  32
                          maxTimestamp: ULong,      //  8  40
                                 crc32: Int,        //  4  44
                               memSize: ULong,      //  8  51
                              reserved: Array[Byte] // 56 107
                            )

case class HdxManifest(header: HdxManifestHeader,


                      ) {

}

case class CodecHeader(blockType: UByte,
                         version: UByte,
              withCrcAndReserved: UShort)