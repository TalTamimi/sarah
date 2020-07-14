package com.example.sarah

import org.junit.Assert
import org.junit.Test
import java.util.concurrent.atomic.AtomicBoolean

class TestingJavaAPI {

    @Test
    fun numbers() {
        Assert.assertEquals("123" > "321", false)
    }

    @Test
    fun numbers2() {
        Assert.assertEquals("123" < "321", true)
    }

    @Test
    fun atomicBoolean() {
        val bool = AtomicBoolean(false)
        Assert.assertTrue(bool.compareAndSet(false, true))
        Assert.assertTrue(bool.get())
    }


    @Test
    fun atomicBoolean2() {
        val bool = AtomicBoolean(true)
        Assert.assertFalse(bool.compareAndSet(false, true))
        Assert.assertTrue(bool.get())
    }

}