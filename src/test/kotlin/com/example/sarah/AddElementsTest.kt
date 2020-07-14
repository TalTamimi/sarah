package com.example.sarah

import com.example.sarah.service.DB
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.newSingleThreadContext
import kotlinx.coroutines.test.resetMain
import kotlinx.coroutines.test.setMain
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.util.ResourceUtils;
import java.io.File

@SpringBootTest
class AddElementsTest {

    companion object{
        val db: DB =DB()
    }


    @Before
    fun clearContext() {
        db.clear()
        recreateFolder( File("./data/"))
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
//                            increase testing dataset size
//                            for(i in 0..2000)
//                                db.put(UUID.randomUUID().toString(), UUID.randomUUID().toString())
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
