import { eventDroppedCounter } from '../../../main/ingestion-queues/metrics'
import { ISOTimestamp, PreIngestionEvent, RawClickhouseWebVitalsEvent, TimestampFormat } from '../../../types'
import { castTimestampOrNow } from '../../../utils/utils'
import { captureIngestionWarning, isNonEmptyString } from '../utils'
import { EventPipelineRunner } from './runner'

const validWebVitalNames = ['LCP', 'FCP', 'INP', 'CLS']

interface ReceivedWebVitalEvent {
    name: string
    value: number
    timestamp: ISOTimestamp
    $session_id: string
    $current_url: string

    // and other things
    [key: string]: string | number
}

export function extractWebVitalsDataStep(
    runner: EventPipelineRunner,
    event: PreIngestionEvent
): Promise<[PreIngestionEvent, Promise<void>[]]> {
    const { eventUuid, teamId } = event

    let acks: Promise<void>[] = []

    try {
        const webVitalsEvents = extractWebVitalsEventsData(event) ?? []

        // eslint-disable-next-line @typescript-eslint/no-floating-promises
        acks = webVitalsEvents.map((rawEvent) => {
            return runner.hub.kafkaProducer.produce({
                topic: runner.hub.CLICKHOUSE_WEB_VITALS_KAFKA_TOPIC,
                key: eventUuid,
                value: Buffer.from(JSON.stringify(rawEvent)),
                waitForAck: true,
            })
        })
    } catch (e) {
        acks.push(
            captureIngestionWarning(runner.hub.kafkaProducer, teamId, 'invalid_web_vitals_data', {
                eventUuid,
            })
        )
    }

    // We don't want to ingest this data to the events table
    delete event.properties['$web_vitals_data']

    return Promise.resolve([event, acks])
}

function extractWebVitalsEventsData(event: PreIngestionEvent): RawClickhouseWebVitalsEvent[] {
    function drop(cause: string): RawClickhouseWebVitalsEvent[] {
        eventDroppedCounter
            .labels({
                event_type: 'web_vitals_event_extraction',
                drop_cause: cause,
            })
            .inc()
        return []
    }

    const { teamId, properties } = event
    const { $web_vitals_data } = properties || {}

    const webVitalsData = $web_vitals_data as any[] | null

    const webVitalsEvents: RawClickhouseWebVitalsEvent[] = []

    if (!webVitalsData || webVitalsData.length === 0) {
        return []
    }

    webVitalsData.forEach((receivedWebVitalEvent: ReceivedWebVitalEvent) => {
        const { name, value, $session_id, $current_url, timestamp, ...properties } = receivedWebVitalEvent

        if (!isNonEmptyString($session_id)) {
            return drop('missing_session_id')
        }

        if (!isNonEmptyString($current_url)) {
            return drop('missing_current_url')
        }

        if (!validWebVitalNames.includes(name)) {
            drop('invalid_web_vital_name')
            return
        }

        const baseEvent: Partial<RawClickhouseWebVitalsEvent> = {
            team_id: teamId,
            timestamp: castTimestampOrNow(timestamp, TimestampFormat.ClickHouse),
            session_id: $session_id,
            current_url: $current_url,
            // remaining properties are enblobbed into one column
            properties: properties ? JSON.stringify(properties) : undefined,
        }

        if (name === 'LCP') {
            baseEvent.lcp = value
        } else if (name === 'FCP') {
            baseEvent.fcp = value
        } else if (name === 'INP') {
            baseEvent.inp = value
        } else if (name === 'CLS') {
            baseEvent.cls = value
        } else {
            drop('invalid_web_vital_name')
            return
        }

        webVitalsEvents.push(baseEvent as RawClickhouseWebVitalsEvent)
    })

    return webVitalsEvents
}