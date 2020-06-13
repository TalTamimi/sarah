package com.example.sarah.domain

data class QueueNode(val row: Row, val fileIndex: Int, val lineIndex: Int) : Comparable<QueueNode> {

    companion object {
        fun fromData(data: Data) = QueueNode(data.rows[data.lineIndex], data.fileIndex, data.lineIndex)
    }

    override fun compareTo(other: QueueNode): Int {
        var thisMarker = 0
        var thatMarker = 0
        val thisLength = this.row.key.length
        val s2Length = other.row.key.length

        if (this.row.key == other.row.key) {
            if (this.fileIndex < other.fileIndex) {
                return -1
            }
            return 1
        }
        while (thisMarker < thisLength && thatMarker < s2Length) {
            val thisChunk = getChunk(this.row.key, thisLength, thisMarker)
            thisMarker += thisChunk.length

            val thatChunk = getChunk(other.row.key, s2Length, thatMarker)
            thatMarker += thatChunk.length

            // If both chunks contain numeric characters, sort them numerically.
            var result: Int
            if (isDigit(thisChunk[0]) && isDigit(thatChunk[0])) {
                // Simple chunk comparison by length.
                val thisChunkLength = thisChunk.length
                result = thisChunkLength - thatChunk.length
                // If equal, the first different number counts.
                if (result == 0) {
                    for (i in 0..thisChunkLength - 1) {
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

        return thisLength - s2Length
    }

    private fun getChunk(string: String, length: Int, marker: Int): String {
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

    private fun isDigit(ch: Char): Boolean {
        return ch in '0'..'9'
    }
}