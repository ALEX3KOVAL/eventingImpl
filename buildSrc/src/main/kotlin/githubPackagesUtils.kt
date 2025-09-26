import org.gradle.api.artifacts.dsl.RepositoryHandler
import org.gradle.authentication.http.BasicAuthentication
import org.gradle.kotlin.dsl.create
import java.net.URI

private fun RepositoryHandler.loadGithubPackage(repo: String) {
    maven {
        name = "GitHubPackages"
        url = URI("https://maven.pkg.github.com/ALEX3KOVAL/$repo")
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

fun RepositoryHandler.loadEventingContractGithubPackage() {
    loadGithubPackage("eventingContract")
}
