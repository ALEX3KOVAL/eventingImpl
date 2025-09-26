plugins {
    id("java-library")
    id("maven-publish")
}

group = "ru.alex3koval"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
    maven {
        name = "GitHubPackages"
        url = uri("https://maven.pkg.github.com/ALEX3KOVAL/eventingContract")
        credentials {
            username = "ALEX3KOVAL"
            password = System.getenv("GITHUB_TOKEN")
        }
        authentication {
            create<BasicAuthentication>("basic")
        }

        content {
            includeGroup("alex3koval")
        }
    }
}

dependencies {
    implementation("org.springframework.kafka:spring-kafka:3.3.9")
    implementation("com.fasterxml.jackson.core:jackson-databind")
    implementation("alex3koval:eventing-contract:1.13.10")

    implementation("org.springframework.cloud:spring-cloud-stream:4.3.0")

    compileOnly("org.projectlombok:lombok:1.18.38")
    annotationProcessor("org.projectlombok:lombok:1.18.38")
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])

            groupId = "alex3koval"
            artifactId = "eventing-impl"
            version = "1.0.0"
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