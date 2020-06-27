package com.example.sarah

import org.assertj.core.internal.Numbers
import org.junit.Assert
import org.junit.Test

class TestStringCompare {

    @Test
    fun numbers() {
        Assert.assertEquals("123">"321" , false)
    }

    @Test
    fun numbers2() {
        Assert.assertEquals("123"<"321" , true)
    }
}