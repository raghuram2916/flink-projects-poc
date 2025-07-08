plugins {
    java
    application
}

repositories {
    mavenCentral()
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

dependencies {
    implementation("org.apache.flink:flink-java:1.18.0")
    implementation("org.apache.flink:flink-streaming-java:1.18.0")
    implementation("org.apache.flink:flink-table-api-java:1.18.0")
    implementation("org.apache.flink:flink-table-planner_2.12:1.18.0")
    implementation("org.apache.flink:flink-connector-mongodb-cdc:3.2.1")
    implementation("org.apache.flink:flink-table-api-java-bridge:1.18.0")
    implementation("org.apache.iceberg:iceberg-flink-runtime-1.18:1.5.0")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.15.2")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.15.2")
    implementation("org.slf4j:slf4j-log4j12:2.0.7")
    implementation("com.github.erosb:everit-json-schema:1.14.2")
    implementation("org.json:json:20230618")
    implementation("com.jayway.jsonpath:json-path:2.8.0")
    implementation("net.minidev:json-smart:2.4.11")
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.10.2")
    testImplementation("org.assertj:assertj-core:3.25.3")
    testImplementation("org.apache.flink:flink-test-utils:1.18.1")
}



application {
    mainClass.set("org.flink.meta.poc.JobLauncher")
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
}
