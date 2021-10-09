package sources

import response.ArticleResponse
import response.SourceResponse
import com.frogobox.api.news.util.NewsUrl
import core.ConsumeApiResponse
import core.FrogoApiCallback
import sources.remote.FrogoApiClient
import io.reactivex.schedulers.Schedulers
import sources.remote.NewsApiService

/**
 * Created by Faisal Amir
 * FrogoBox Inc License
 * =========================================
 * consumable-code-news-api
 * Copyright (C) 15/03/2020.
 * All rights reserved
 * -----------------------------------------
 * Name     : Muhammad Faisal Amir
 * E-mail   : faisalamircs@gmail.com
 * Github   : github.com/amirisback
 * LinkedIn : linkedin.com/in/faisalamircs
 * -----------------------------------------
 * FrogoBox Software Industries
 * com.frogobox.frogoconsumeapi.news.data.source
 *
 */
object NewsRepository : NewsDataSource {

    private val TAG = NewsRepository::class.java.simpleName
    private var newsApiService = FrogoApiClient.create<NewsApiService>(NewsUrl.BASE_URL)

    override fun getTopHeadline(
        apiKey: String,
        q: String?,
        sources: String?,
        category: String?,
        country: String?,
        pageSize: Int?,
        page: Int?,
        callback: ConsumeApiResponse<ArticleResponse>
    ) {
        newsApiService.getTopHeadline(apiKey, q, sources, category, country, pageSize, page)
            .subscribeOn(Schedulers.io())
            .doOnSubscribe { callback.onShowProgress() }
            .doOnTerminate { callback.onHideProgress() }
            .observeOn(Schedulers.single())
            .subscribe(object : FrogoApiCallback<ArticleResponse>() {
                override fun onSuccess(data: ArticleResponse) {
                    callback.onSuccess(data)
                }

                override fun onFailure(code: Int, errorMessage: String) {
                    callback.onFailed(code, errorMessage)
                }
            })
    }

    override fun getEverythings(
        apiKey: String,
        q: String?,
        from: String?,
        to: String?,
        qInTitle: String?,
        sources: String?,
        domains: String?,
        excludeDomains: String?,
        language: String?,
        sortBy: String?,
        pageSize: Int?,
        page: Int?,
        callback: ConsumeApiResponse<ArticleResponse>
    ) {
        newsApiService.getEverythings(
            apiKey,
            q,
            from,
            to,
            qInTitle,
            sources,
            domains,
            excludeDomains,
            language,
            sortBy,
            pageSize,
            page
        )
            .subscribeOn(Schedulers.io())
            .doOnSubscribe { callback.onShowProgress() }
            .doOnTerminate { callback.onHideProgress() }
            .observeOn(Schedulers.trampoline())
            .subscribe(object : FrogoApiCallback<ArticleResponse>() {
                override fun onSuccess(data: ArticleResponse) {
                    callback.onSuccess(data)
                }

                override fun onFailure(code: Int, errorMessage: String) {
                    callback.onFailed(code, errorMessage)
                }
            })
    }

    override fun getSources(
        apiKey: String,
        language: String,
        country: String,
        category: String,
        callback: ConsumeApiResponse<SourceResponse>
    ) {
        newsApiService.getSources(apiKey, language, country, category)
            .subscribeOn(Schedulers.io())
            .doOnSubscribe { callback.onShowProgress() }
            .doOnTerminate { callback.onHideProgress() }
            .observeOn(Schedulers.trampoline())
            .subscribe(object : FrogoApiCallback<SourceResponse>() {
                override fun onSuccess(data: SourceResponse) {
                    callback.onSuccess(data)
                }

                override fun onFailure(code: Int, errorMessage: String) {
                    callback.onFailed(code, errorMessage)
                }
            })
    }
}