import { ClientInterface, ClientFetchManyOptions, ClientRequestOptions, BaseClient, BaseClientOptions, FaliedAttempt, HttpResponse,InvalidStatusCodeError, defaultUserAgent, delay } from "@xcrap/core"
import { ExtendedExtendOptions, GotScraping, Response } from "got-scraping"
import { loadEsm } from "load-esm"

export type GotScrapingProxy = string

export type GotScrapingRequestOptions = ClientRequestOptions & ExtendedExtendOptions

export type GotScrapingFetchManyOptions = ClientFetchManyOptions<GotScrapingRequestOptions>

export interface GotScrapingClientOptions extends BaseClientOptions<GotScrapingProxy>, Omit<ExtendedExtendOptions, "proxyUrl"> {}

export class GotScrapingClient extends BaseClient<GotScrapingProxy> implements ClientInterface {
    protected gotScrapingInstance: GotScraping | undefined
    protected readonly options: GotScrapingClientOptions

    constructor(options: GotScrapingClientOptions = {}) {
        super(options)
        this.options = options
    }

    protected async initGotScraping() {
        const module = await loadEsm("got-scraping")
        const gotScraping: GotScraping = module.gotScraping

        // userAgent, proxy and proxyUrl are Xcrap options resolved on every request, not got options
        const { userAgent: _userAgent, proxy: _proxy, proxyUrl: _proxyUrl, ...gotOptions } = this.options as GotScrapingClientOptions & { proxyUrl?: unknown }

        this.gotScrapingInstance = gotScraping.extend(gotOptions)
    }

    protected async ensureGotScraping(): Promise<void> {
        if (!this.gotScrapingInstance) {
            await this.initGotScraping()
        }
    }

    async fetch({
        url,
        maxRetries = 0,
        retries = 0,
        retryDelay,
        method = "GET",
        ...gotOptions
    }: GotScrapingRequestOptions): Promise<HttpResponse> {
        await this.ensureGotScraping()

        const failedAttempts: FaliedAttempt[] = []

        const attemptRequest = async (currentRetry: number): Promise<HttpResponse> => {
            let response: Response | undefined
            let errorMessage: string

            try {
                const fullUrl = this.currentProxyUrl ? `${this.currentProxyUrl}${url}` : url

                response = await this.gotScrapingInstance!({
                    ...gotOptions,
                    url: fullUrl,
                    method: method,
                    headers: {
                        "User-Agent": this.currentUserAgent ?? defaultUserAgent,
                        ...gotOptions.headers
                    },
                    proxyUrl: this.currentProxy,
                }) as Response

                if (this.isSuccess(response.statusCode)) {
                    return this.toHttpResponse(response, currentRetry, failedAttempts)
                }

                errorMessage = new InvalidStatusCodeError(response.statusCode, fullUrl).message
            } catch (error: any) {
                errorMessage = error instanceof Error ? error.message : "Unknown error"
                response = error.response
            }

            failedAttempts.push({ error: errorMessage, timestamp: new Date() })

            if (currentRetry < maxRetries) {
                if (retryDelay !== undefined && retryDelay > 0) {
                    await delay(retryDelay)
                }

                return await attemptRequest(currentRetry + 1)
            }

            if (response) {
                return this.toHttpResponse(response, currentRetry, failedAttempts)
            }

            return new HttpResponse({
                status: 500,
                statusText: "Request Failed",
                body: errorMessage,
                headers: {},
                attempts: currentRetry + 1,
                failedAttempts,
            })
        }

        return await attemptRequest(retries)
    }

    protected toHttpResponse(response: Response, currentRetry: number, failedAttempts: FaliedAttempt[]): HttpResponse {
        return new HttpResponse({
            status: response.statusCode,
            statusText: response.statusMessage || "",
            headers: response.headers,
            body: response.body,
            attempts: currentRetry + 1,
            failedAttempts,
        })
    }

    async fetchMany({ requests, concurrency, requestDelay }: GotScrapingFetchManyOptions): Promise<HttpResponse[]> {
        const results: HttpResponse[] = []
        const executing: Promise<void>[] = []

        for (let i = 0; i < requests.length; i++) {
            const promise = this.executeRequest({
                request: requests[i],
                index: i,
                requestDelay: requestDelay,
                results: results
            }).then(() => undefined)

            executing.push(promise)

            if (this.shouldThrottle(executing, concurrency)) {
                await this.handleConcurrency(executing)
            }
        }

        await Promise.all(executing)

        return results
    }
}
