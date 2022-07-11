import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.7.0"
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

kotlin {
    jvmToolchain {
        languageVersion.set(JavaLanguageVersion.of(18))
    }
}

dependencies {
    testImplementation(kotlin("test"))
    testImplementation("io.kotest:kotest-runner-junit5:5.3.1")
    testImplementation("io.kotest.extensions:kotest-extensions-embedded-kafka:1.0.6")
    implementation("org.apache.kafka:kafka-clients:2.8.0")
    implementation("ch.qos.logback:logback-classic:1.2.11")
    implementation("ch.qos.logback:logback-core:1.2.11")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.6.2")
    implementation("io.ktor:ktor-network:2.0.3")
    implementation("io.ktor:ktor-network-tls:2.0.3")
    implementation("io.ktor:ktor-network-tls:2.0.3:sources")
    implementation("io.ktor:ktor-network-tls-jvm:2.0.3:sources")
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "17"
}