#!/bin/bash
@echo off

rem Compile DBCoursework.java
javac DBCoursework.java

REM Run DBCoursework with each of the drivers dependencies
java -cp ".;sqlite-jdbc-3.45.1.0.jar;slf4j-api-1.7.36.jar" DBCoursework

$SHELL