import json
from typing import Any, Type

from rest_framework import mixins, permissions, serializers, viewsets

from posthog.api.routing import StructuredViewSetMixin
from posthog.api.tagged_item import TaggedItemSerializerMixin, WritableSerializerMethodField
from posthog.constants import GROUP_TYPES_LIMIT, AvailableFeature
from posthog.exceptions import EnterpriseFeatureException
from posthog.filters import TermSearchFilterBackend, term_search_filter_sql
from posthog.models import PropertyDefinition
from posthog.permissions import OrganizationMemberPermissions, TeamMemberAccessPermission

# Properties generated by ingestion we don't want to show to users
HIDDEN_PROPERTY_DEFINITIONS = set(
    [
        # distinct_id is set in properties by some libraries
        "distinct_id",
        # used for updating properties
        "$set",
        "$set_once",
        # Group Analytics
        "$groups",
        "$group_type",
        "$group_key",
        "$group_set",
    ]
    + [f"$group_{i}" for i in range(GROUP_TYPES_LIMIT)]
)


class PropertyDefinitionSerializer(TaggedItemSerializerMixin, serializers.ModelSerializer):
    class Meta:
        model = PropertyDefinition
        fields = (
            "id",
            "name",
            "is_numerical",
            "query_usage_30_day",
            "property_type",
            "property_type_format",
            # This is a calculated property, used only when "event_names" is passed to the API.
            "is_event_property",
            "tags_v2",
            "tags",
        )

    def update(self, property_definition: PropertyDefinition, validated_data):
        raise EnterpriseFeatureException()


class PropertyDefinitionViewSet(
    StructuredViewSetMixin,
    mixins.ListModelMixin,
    mixins.RetrieveModelMixin,
    mixins.UpdateModelMixin,
    viewsets.GenericViewSet,
):
    serializer_class = PropertyDefinitionSerializer
    permission_classes = [permissions.IsAuthenticated, OrganizationMemberPermissions, TeamMemberAccessPermission]
    lookup_field = "id"
    filter_backends = [TermSearchFilterBackend]
    ordering = "name"
    search_fields = ["name"]

    def get_queryset(self):
        use_entreprise_taxonomy = self.request.user.organization.is_feature_available(AvailableFeature.INGESTION_TAXONOMY)  # type: ignore
        if use_entreprise_taxonomy:
            try:
                from ee.models.property_definition import EnterprisePropertyDefinition
            except ImportError:
                use_entreprise_taxonomy = False

        properties_to_filter = self.request.GET.get("properties", None)
        if properties_to_filter:
            names = tuple(properties_to_filter.split(","))
            name_filter = "AND name IN %(names)s"
        else:
            names = ()
            name_filter = ""

        # Passed as JSON instead of duplicate properties like event_names[] to work with frontend's combineUrl
        event_names = self.request.GET.get("event_names", None)
        if event_names:
            event_names = json.loads(event_names)

        if event_names and len(event_names) > 0:
            event_property_field = "(SELECT count(1) > 0 FROM posthog_eventproperty WHERE posthog_eventproperty.team_id=posthog_propertydefinition.team_id AND posthog_eventproperty.event IN %(event_names)s AND posthog_eventproperty.property = posthog_propertydefinition.name)"
        else:
            event_property_field = "NULL"

        search = self.request.GET.get("search", None)
        search_query, search_kwargs = term_search_filter_sql(self.search_fields, search)

        params = {
            "event_names": tuple(event_names or []),
            "names": names,
            "team_id": self.team_id,
            "excluded_properties": tuple(HIDDEN_PROPERTY_DEFINITIONS),
            **search_kwargs,
        }

        if use_entreprise_taxonomy:
            return EnterprisePropertyDefinition.objects.raw(
                f"""
                SELECT posthog_propertydefinition.*,
                       ee_enterprisepropertydefinition.*, 
                       {event_property_field} AS is_event_property
                FROM posthog_propertydefinition
                LEFT JOIN ee_enterprisepropertydefinition ON ee_enterprisepropertydefinition.propertydefinition_ptr_id=posthog_propertydefinition.id
                WHERE posthog_propertydefinition.team_id = %(team_id)s AND name NOT IN %(excluded_properties)s {name_filter} {search_query}
                GROUP BY posthog_propertydefinition.id, ee_enterprisepropertydefinition.propertydefinition_ptr_id
                ORDER BY is_event_property DESC, query_usage_30_day DESC NULLS LAST, name ASC
                """,
                params=params,
            )
        else:
            return PropertyDefinition.objects.raw(
                f"""
                SELECT posthog_propertydefinition.*, {event_property_field} AS is_event_property
                FROM posthog_propertydefinition
                WHERE posthog_propertydefinition.team_id = %(team_id)s AND name NOT IN %(excluded_properties)s {name_filter} {search_query}
                ORDER BY is_event_property DESC, query_usage_30_day DESC NULLS LAST, name ASC
                """,
                params=params,
            )

    def get_serializer_class(self) -> Type[serializers.ModelSerializer]:
        serializer_class = self.serializer_class
        if self.request.user.organization.is_feature_available(AvailableFeature.INGESTION_TAXONOMY):  # type: ignore
            try:
                from ee.api.ee_property_definition import EnterprisePropertyDefinitionSerializer
            except ImportError:
                pass
            else:
                serializer_class = EnterprisePropertyDefinitionSerializer  # type: ignore
        return serializer_class

    def get_object(self):
        id = self.kwargs["id"]
        if self.request.user.organization.is_feature_available(AvailableFeature.INGESTION_TAXONOMY):  # type: ignore
            try:
                from ee.models.property_definition import EnterprisePropertyDefinition
            except ImportError:
                pass
            else:
                enterprise_property = EnterprisePropertyDefinition.objects.filter(id=id).first()
                if enterprise_property:
                    return enterprise_property
                non_enterprise_property = PropertyDefinition.objects.get(id=id)
                new_enterprise_property = EnterprisePropertyDefinition(
                    propertydefinition_ptr_id=non_enterprise_property.id, description=""
                )
                new_enterprise_property.__dict__.update(non_enterprise_property.__dict__)
                new_enterprise_property.save()
                return new_enterprise_property
        return PropertyDefinition.objects.get(id=id)
