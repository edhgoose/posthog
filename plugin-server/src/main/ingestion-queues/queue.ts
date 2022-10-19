import { Hub, PipelineEvent, PostIngestionEvent, WorkerMethods } from '../../types'
import { status } from '../../utils/status'
import { IngestionConsumer } from './kafka-queue'
import { workerTasks } from '../../worker/tasks'

interface Queues {
    ingestion: IngestionConsumer | null
}

export function pauseQueueIfWorkerFull(
    pause: undefined | (() => void | Promise<void>),
    server: Hub,
    piscina?: any
): void {
    if (pause && (piscina?.queueSize || 0) > (server.WORKER_CONCURRENCY || 4) * (server.WORKER_CONCURRENCY || 4)) {
        void pause()
    }
}

export async function startQueues(server: Hub, workerMethods: Partial<WorkerMethods> = {}): Promise<Queues> {
    const mergedWorkerMethods = {
        runAsyncHandlersEventPipeline: (event: PostIngestionEvent) => {
            server.lastActivity = new Date().valueOf()
            server.lastActivityType = 'runAsyncHandlersEventPipeline'
            return workerTasks['runAsyncHandlersEventPipeline'](server, { event })
        },
        runEventPipeline: (event: PipelineEvent) => {
            server.lastActivity = new Date().valueOf()
            server.lastActivityType = 'runEventPipeline'
            return workerTasks['runEventPipeline'](server, { event })
        },
        runLightweightCaptureEndpointEventPipeline: (event: PipelineEvent) => {
            server.lastActivity = new Date().valueOf()
            server.lastActivityType = 'runLightweightCaptureEndpointEventPipeline'
            return workerTasks['runLightweightCaptureEndpointEventPipeline'](server, { event })
        },
        ...workerMethods,
    }

    try {
        const queues: Queues = {
            ingestion: await startQueueKafka(server, mergedWorkerMethods),
        }
        return queues
    } catch (error) {
        status.error('💥', 'Failed to start event queue:\n', error)
        throw error
    }
}

async function startQueueKafka(server: Hub, workerMethods: WorkerMethods): Promise<IngestionConsumer | null> {
    if (!server.capabilities.ingestion && !server.capabilities.processAsyncHandlers) {
        return null
    }

    const ingestionConsumer = new IngestionConsumer(server, workerMethods)
    await ingestionConsumer.start()

    return ingestionConsumer
}
