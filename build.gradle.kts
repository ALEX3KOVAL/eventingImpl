plugins {
    id("java-library")
    id("maven-publish")
}

group = "ru.alex3koval"
version = "1.0.6"

repositories {
    mavenCentral()
    loadEventingContractGithubPackage()
}

dependencies {
    implementation("org.springframework.kafka:spring-kafka:3.3.9")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.20.0")
    implementation("io.projectreactor:reactor-core:3.4.40")
    implementation("alex3koval:eventing-contract:latest.release")
    implementation("ch.qos.logback:logback-classic:1.5.18")
    implementation("org.springframework.kafka:spring-kafka")
    implementation("org.springframework.boot:spring-boot-autoconfigure:3.5.4")

    compileOnly("org.projectlombok:lombok:1.18.38")
    annotationProcessor("org.projectlombok:lombok:1.18.38")
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])

            groupId = "alex3koval"
            artifactId = "eventing-impl"
            version = "1.0.6"
        }
    }

    repositories {
        maven {
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/ALEX3KOVAL/eventingImpl")

            credentials {
                username = "ALEX3KOVAL"
                password = System.getenv("GITHUB_TOKEN")
            }
        }
    }
}
