package com.varisahayak.data.repository

import android.util.Log
import com.varisahayak.core.common.DispatcherProvider
import com.varisahayak.core.network.ConnectivityObserver
import com.varisahayak.data.remote.dto.ClassificationDto
import com.varisahayak.domain.model.IncidentCategory
import com.varisahayak.domain.repository.ClassificationRepository
import com.varisahayak.domain.usecase.AiSuggestion
import io.github.jan.supabase.SupabaseClient
import io.github.jan.supabase.functions.functions
import io.ktor.client.call.body
import io.ktor.client.request.setBody
import io.ktor.http.ContentType
import io.ktor.http.contentType
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.withContext
import kotlinx.serialization.Serializable
import javax.inject.Inject
import javax.inject.Singleton

@Serializable
private data class ClassifyRequest(
    val description: String,
    val category: String? = null,
    val incident_client_id: String? = null,
)

/**
 * Calls the `classify-incident` edge function.
 *
 * Every path returns null rather than an error. That is the whole design: the caller has
 * one case to handle — a suggestion arrived, or it did not — and no failure here can
 * become a message in front of a volunteer who is trying to file an incident.
 *
 * There is no Gemini key here, no model name, and no call to
 * `generativelanguage.googleapis.com`. The key exists only inside the edge function.
 */
@Singleton
class ClassificationRepositoryImpl @Inject constructor(
    private val supabase: SupabaseClient,
    private val connectivity: ConnectivityObserver,
    private val dispatchers: DispatcherProvider,
) : ClassificationRepository {

    override suspend fun suggest(
        description: String,
        selectedCategory: IncidentCategory?,
        incidentClientId: String?,
    ): AiSuggestion? = withContext(dispatchers.io) {
        if (description.isBlank()) return@withContext null

        // Checked up front so an offline device does not spend a timeout discovering
        // something it already knows. Enrichment is the first thing to give up when the
        // connection is bad, because nothing depends on it.
        if (!connectivity.isCurrentlyOnline()) return@withContext null

        try {
            val response = supabase.functions.invoke("classify-incident") {
                contentType(ContentType.Application.Json)
                setBody(
                    ClassifyRequest(
                        description = description.trim(),
                        category = selectedCategory?.wireName,
                        incident_client_id = incidentClientId,
                    ),
                )
            }

            val dto = response.body<ClassificationDto>()

            // available:false is the function's normal answer whenever Gemini was
            // unreachable, unconfigured, or returned something that failed validation.
            if (!dto.available) return@withContext null

            val category = IncidentCategory.fromWire(dto.category)
            val severity = dto.severity ?: return@withContext null

            // Validated server-side already; re-checked here because a suggestion that
            // survives into PriorityEngine with an out-of-range severity would score
            // unpredictably, and this is cheaper than trusting the boundary.
            if (severity !in 1..5) return@withContext null

            AiSuggestion(
                category = category,
                severity = severity,
                rationale = dto.rationale,
                isUsable = true,
            )
        } catch (cancellation: CancellationException) {
            throw cancellation
        } catch (error: Exception) {
            // Logged, never surfaced. A volunteer standing next to somebody who needs help
            // must not be shown an error about a classifier.
            Log.d(TAG, "Classification unavailable: ${error.javaClass.simpleName}")
            null
        }
    }

    private companion object {
        const val TAG = "ClassificationRepository"
    }
}
