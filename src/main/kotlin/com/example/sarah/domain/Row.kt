package com.example.sarah.domain


data class Row(val key: String, val value: String, val index: String) {

    companion object {
        fun RowContructor(line: String, index: String): Row {
            val keyValue = line.split('\t')
            return Row(keyValue[0], keyValue[1], index)
        }
    }

    override fun toString(): String {
        return "$key\t$value\n"

    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        other as Row
        if (key != other.key) return false
        if (index != other.index) return false

        return true
    }

    override fun hashCode(): Int {
        var result = key.hashCode()
        result = 31 * result + index.hashCode()
        return result
    }


}
