package com.example.sarah.domain

enum class MergeAlgorithm : Comparator<Row> {
    BUFFERED_MERGE_SORT {
        override fun compare(s1: Row, other: Row): Int {
            var thisMarker = 0
            var thatMarker = 0
            val thisLength = s1.key.length
            val s2Length = other.key.length


            while (thisMarker < thisLength && thatMarker < s2Length) {
                val thisChunk = getChunk(s1.key, thisLength, thisMarker)
                thisMarker += thisChunk.length

                val thatChunk = getChunk(other.key, s2Length, thatMarker)
                thatMarker += thatChunk.length

                // If both chunks contain numeric characters, sort them numerically.
                var result: Int
                if (isDigit(thisChunk[0]) && isDigit(thatChunk[0])) {
                    // Simple chunk comparison by length.
                    val thisChunkLength = thisChunk.length
                    result = thisChunkLength - thatChunk.length
                    // If equal, the first different number counts.
                    if (result == 0) {
                        for (i in 0 until thisChunkLength) {
                            result = thisChunk[i] - thatChunk[i]
                            if (result != 0) {
                                return result
                            }
                        }
                    }
                } else {
                    result = thisChunk.compareTo(thatChunk)
                }

                if (result != 0) {
                    return result
                }
            }

            return 0
        }
    },
    IN_MEMORY_MERGE_SORT {
        override fun compare(s1: Row, other: Row): Int {
            var thisMarker = 0
            var thatMarker = 0
            val thisLength = s1.key.length
            val s2Length = other.key.length


            while (thisMarker < thisLength && thatMarker < s2Length) {
                val thisChunk = getChunk(s1.key, thisLength, thisMarker)
                thisMarker += thisChunk.length

                val thatChunk = getChunk(other.key, s2Length, thatMarker)
                thatMarker += thatChunk.length

                // If both chunks contain numeric characters, sort them numerically.
                var result: Int
                if (isDigit(thisChunk[0]) && isDigit(thatChunk[0])) {
                    // Simple chunk comparison by length.
                    val thisChunkLength = thisChunk.length
                    result = thisChunkLength - thatChunk.length
                    // If equal, the first different number counts.
                    if (result == 0) {
                        for (i in 0 until thisChunkLength) {
                            result = thisChunk[i] - thatChunk[i]
                            if (result != 0) {
                                return result
                            }
                        }
                    }
                } else {
                    result = thisChunk.compareTo(thatChunk)
                }

                if (result != 0) {
                    return result
                }
            }

            return -1
        }
    };


    internal fun getChunk(string: String, length: Int, marker: Int): String {
        var current = marker
        val chunk = StringBuilder()
        var c = string[current]
        chunk.append(c)
        current++
        if (isDigit(c)) {
            while (current < length) {
                c = string[current]
                if (!isDigit(c)) {
                    break
                }
                chunk.append(c)
                current++
            }
        } else {
            while (current < length) {
                c = string[current]
                if (isDigit(c)) {
                    break
                }
                chunk.append(c)
                current++
            }
        }
        return chunk.toString()
    }

    internal fun isDigit(ch: Char): Boolean {
        return ch in '0'..'9'
    }
}
