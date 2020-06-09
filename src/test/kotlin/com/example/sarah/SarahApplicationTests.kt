package com.example.sarah

import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.util.ResourceUtils;
import java.io.File

@SpringBootTest
class SarahApplicationTests() {
    companion object {
        val db = DB()
    }

    @Before
    fun clearContext() {
        db.clear()
        val dataFolder = File("./data/")
        dataFolder.deleteRecursively()
        dataFolder.mkdir()
        loadData()
    }

    fun loadData() {
        ResourceUtils.getFile("classpath:sample")
                .walkBottomUp()
                .filter { it.isFile }
                .forEach { file ->
                    file.useLines { sequence ->
                        sequence.map { s ->
                            s.split('\t')
                        }.forEach {
                            db.put(it[0], it[1])
                        }
                    }
                }
    }
    @Test
    fun findLast() {
        assertEquals("hello3", db.get("test3"))
    }


    @Test
    fun findMiddle() {
        assertEquals("hello2", db.get("test2"))
    }

    @Test
    fun findFirst() {
        assertEquals("hello1", db.get("test1"))
    }
}
