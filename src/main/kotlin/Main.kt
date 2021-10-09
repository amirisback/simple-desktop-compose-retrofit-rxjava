// Copyright 2000-2021 JetBrains s.r.o. and contributors. Use of this source code is governed by the Apache 2.0 license that can be found in the LICENSE file.
import androidx.compose.desktop.DesktopMaterialTheme
import androidx.compose.desktop.ui.tooling.preview.Preview
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.material.Button
import androidx.compose.material.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.window.Window
import androidx.compose.ui.window.application
import com.frogobox.api.news.util.NewsConstant.CATEGORY_HEALTH
import com.frogobox.api.news.util.NewsConstant.COUNTRY_ID
import com.frogobox.api.news.util.NewsUrl
import core.ConsumeApiResponse
import model.Article
import response.ArticleResponse

@Composable
@Preview
fun App() {
    DesktopMaterialTheme {
        RecyclerView()
    }
}

fun main() = application {
    Window(onCloseRequest = ::exitApplication) {
        App()
    }
}

@Composable
fun RecyclerView() {

    var newsState : List<Article> by remember { mutableStateOf(emptyList()) }

    val consumeNewsApi = ConsumeNewsApi(NewsUrl.API_KEY) // Your API_KEY
    consumeNewsApi.getTopHeadline( // Adding Base Parameter on main function
        null,
        null,
        CATEGORY_HEALTH,
        COUNTRY_ID,
        null,
        null,
        object : ConsumeApiResponse<ArticleResponse> {
            override fun onSuccess(data: ArticleResponse) {
                for (i in data.articles?.indices!!) {
                    println("${i + 1}.\t ${data.articles?.get(i)?.title}")
                }
                newsState = data.articles!!
            }

            override fun onFailed(statusCode: Int, errorMessage: String?) {
                // Your failed to do
            }

            override fun onShowProgress() {
                // Your Progress Show
                println("Show Progress")
            }

            override fun onHideProgress() {
                // Your Progress Hide
                println("Hide Progress")
            }

        })

    LazyColumn {
        items(newsState.size) { index ->
            newsState[index].title?.let { Text("${index + 1}.\t\t $it") }
        }
    }

}