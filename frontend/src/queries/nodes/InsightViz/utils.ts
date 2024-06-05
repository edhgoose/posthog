import equal from 'fast-deep-equal'
import { PERCENT_STACK_VIEW_DISPLAY_TYPE } from 'lib/constants'
import { getEventNamesForAction, isEmptyObject } from 'lib/utils'

import {
    ActionsNode,
    BreakdownFilter,
    CompareFilter,
    DataWarehouseNode,
    EventsNode,
    InsightQueryNode,
    Node,
    TrendsQuery,
} from '~/queries/schema'
import {
    isInsightQueryWithBreakdown,
    isInsightQueryWithCompare,
    isInsightQueryWithSeries,
    isLifecycleQuery,
    isStickinessQuery,
    isTrendsQuery,
} from '~/queries/utils'
import { ActionType, ChartDisplayType, InsightModel, IntervalType } from '~/types'

import { filtersToQueryNode } from '../InsightQuery/utils/filtersToQueryNode'
import { seriesToActionsAndEvents } from '../InsightQuery/utils/queryNodeToFilter'

export const getAllEventNames = (query: InsightQueryNode, allActions: ActionType[]): string[] => {
    const { actions, events } = seriesToActionsAndEvents((query as TrendsQuery).series || [])

    // If there's a "All events" entity, don't filter by event names.
    if (events.find((e) => e.id === null)) {
        return []
    }

    const allEvents = [
        ...events.map((e) => String(e.id)),
        ...actions.flatMap((action) => getEventNamesForAction(action.id as string | number, allActions)),
    ]

    // remove duplicates and empty events
    return Array.from(new Set(allEvents.filter((a): a is string => !!a)))
}

export const getDisplay = (query: InsightQueryNode): ChartDisplayType | undefined => {
    if (isStickinessQuery(query)) {
        return query.stickinessFilter?.display
    } else if (isTrendsQuery(query)) {
        return query.trendsFilter?.display
    }
    return undefined
}

export const getFormula = (query: InsightQueryNode): string | undefined => {
    if (isTrendsQuery(query)) {
        return query.trendsFilter?.formula
    }
    return undefined
}

export const getSeries = (query: InsightQueryNode): (EventsNode | ActionsNode | DataWarehouseNode)[] | undefined => {
    if (isInsightQueryWithSeries(query)) {
        return query.series
    }
    return undefined
}

export const getInterval = (query: InsightQueryNode): IntervalType | undefined => {
    if (isInsightQueryWithSeries(query)) {
        return query.interval
    }
    return undefined
}

export const getBreakdown = (query: InsightQueryNode): BreakdownFilter | undefined => {
    if (isInsightQueryWithBreakdown(query)) {
        return query.breakdownFilter
    }
    return undefined
}

export const getCompareFilter = (query: InsightQueryNode): CompareFilter | undefined => {
    if (isInsightQueryWithCompare(query)) {
        return query.compareFilter
    }
    return undefined
}

export const getShowLegend = (query: InsightQueryNode): boolean | undefined => {
    if (isStickinessQuery(query)) {
        return query.stickinessFilter?.showLegend
    } else if (isTrendsQuery(query)) {
        return query.trendsFilter?.showLegend
    } else if (isLifecycleQuery(query)) {
        return query.lifecycleFilter?.showLegend
    }
    return undefined
}

export const getShowValuesOnSeries = (query: InsightQueryNode): boolean | undefined => {
    if (isLifecycleQuery(query)) {
        return query.lifecycleFilter?.showValuesOnSeries
    } else if (isStickinessQuery(query)) {
        return query.stickinessFilter?.showValuesOnSeries
    } else if (isTrendsQuery(query)) {
        return query.trendsFilter?.showValuesOnSeries
    }
    return undefined
}

export const getShowLabelsOnSeries = (query: InsightQueryNode): boolean | undefined => {
    if (isTrendsQuery(query)) {
        return query.trendsFilter?.showLabelsOnSeries
    }
    return undefined
}

export const supportsPercentStackView = (q: InsightQueryNode | null | undefined): boolean =>
    isTrendsQuery(q) && PERCENT_STACK_VIEW_DISPLAY_TYPE.includes(getDisplay(q) || ChartDisplayType.ActionsLineGraph)

export const getShowPercentStackView = (query: InsightQueryNode): boolean | undefined =>
    supportsPercentStackView(query) && (query as TrendsQuery)?.trendsFilter?.showPercentStackView

export const getCachedResults = (
    cachedInsight: Partial<InsightModel> | undefined | null,
    query: InsightQueryNode
): Partial<InsightModel> | undefined => {
    if (!cachedInsight) {
        return undefined
    }

    let cachedQueryNode: Node | undefined

    if (cachedInsight.query) {
        cachedQueryNode = cachedInsight.query
        if ('source' in cachedInsight.query) {
            cachedQueryNode = cachedInsight.query.source as Node
        }
    } else if (cachedInsight.filters && !isEmptyObject(cachedInsight.filters)) {
        cachedQueryNode = filtersToQueryNode(cachedInsight.filters)
    } else {
        return undefined
    }

    // only set the cached result when the filters match the currently set ones
    if (!equal(cachedQueryNode, query)) {
        return undefined
    }

    return cachedInsight
}
