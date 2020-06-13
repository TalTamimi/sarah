package com.example.sarah.domain


data class Row(val key: String, val value: String) : Comparable<Row> {

    override fun toString(): String {
        return "$key\t$value\n"
    }

    override fun equals(other: Any?): Boolean {
        return this.key == (other as Row).key
    }

    override fun hashCode(): Int {
        var result = key.hashCode()
        result *= 31
        return result
    }

    override fun compareTo(other: Row): Int {
        return when {
            this.key < other.key -> -1
            this.key > other.key -> 1
            else -> 0
        }
    }

}
