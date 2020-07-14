package com.example.sarah

import java.io.File

    fun recreateFolder(sstDir: File) {
        sstDir.deleteRecursively()
        sstDir.mkdir()
    }
