package pl.allegro.tech.hermes.api;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.OptBoolean;

import java.time.Instant;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

@JsonIgnoreProperties(value = {"createdAt", "modifiedAt"}, allowGetters = true)
public class Topic {

    public static final int MIN_MESSAGE_SIZE = 1024;
    public static final int MAX_MESSAGE_SIZE = 2 * 1024 * 1024;
    public static final String DEFAULT_SCHEMA_ID_SERIALIZATION_ENABLED_KEY = "defaultSchemaIdAwareSerializationEnabled";
    private static final int DEFAULT_MAX_MESSAGE_SIZE = 50 * 1024;
    private final boolean schemaIdAwareSerializationEnabled;
    private final TopicDataOfflineStorage offlineStorage;
    @Valid
    @NotNull
    private TopicName name;
    @NotNull
    private String description;
    @Valid
    @NotNull
    private OwnerId owner;
    private boolean jsonToAvroDryRunEnabled = false;
    @NotNull
    private Ack ack;
    @NotNull
    private ContentType contentType;
    @Min(MIN_MESSAGE_SIZE)
    @Max(MAX_MESSAGE_SIZE)
    private int maxMessageSize;
    @Valid
    @NotNull
    private RetentionTime retentionTime = RetentionTime.of(1, TimeUnit.DAYS);
    private boolean trackingEnabled = false;
    private boolean migratedFromJsonType = false;
    private boolean subscribingRestricted = false;

    private PublishingAuth publishingAuth;
    @Valid
    private Set<TopicLabel> labels;
    private Instant createdAt;
    private Instant modifiedAt;

    public Topic(TopicName name, String description, OwnerId owner, RetentionTime retentionTime,
                 boolean migratedFromJsonType, Ack ack, boolean trackingEnabled, ContentType contentType, boolean jsonToAvroDryRunEnabled,
                 @JacksonInject(value = DEFAULT_SCHEMA_ID_SERIALIZATION_ENABLED_KEY, useInput = OptBoolean.TRUE)
                 Boolean schemaIdAwareSerializationEnabled,
                 int maxMessageSize, PublishingAuth publishingAuth, boolean subscribingRestricted,
                 TopicDataOfflineStorage offlineStorage, Set<TopicLabel> labels, Instant createdAt, Instant modifiedAt) {
        this.name = name;
        this.description = description;
        this.owner = owner;
        this.retentionTime = retentionTime;
        this.ack = (ack == null ? Ack.LEADER : ack);
        this.trackingEnabled = trackingEnabled;
        this.migratedFromJsonType = migratedFromJsonType;
        this.contentType = contentType;
        this.jsonToAvroDryRunEnabled = jsonToAvroDryRunEnabled;
        this.schemaIdAwareSerializationEnabled = schemaIdAwareSerializationEnabled;
        this.maxMessageSize = maxMessageSize;
        this.publishingAuth = publishingAuth;
        this.subscribingRestricted = subscribingRestricted;
        this.offlineStorage = offlineStorage;
        this.labels = labels;
        this.createdAt = createdAt;
        this.modifiedAt = modifiedAt;
    }

    @JsonCreator
    public Topic(
            @JsonProperty("name") String qualifiedName,
            @JsonProperty("description") String description,
            @JsonProperty("owner") OwnerId owner,
            @JsonProperty("retentionTime") RetentionTime retentionTime,
            @JsonProperty("jsonToAvroDryRun") boolean jsonToAvroDryRunEnabled,
            @JsonProperty("ack") Ack ack,
            @JsonProperty("trackingEnabled") boolean trackingEnabled,
            @JsonProperty("migratedFromJsonType") boolean migratedFromJsonType,
            @JsonProperty("schemaIdAwareSerializationEnabled")
            @JacksonInject(value = DEFAULT_SCHEMA_ID_SERIALIZATION_ENABLED_KEY, useInput = OptBoolean.TRUE)
            Boolean schemaIdAwareSerializationEnabled,
            @JsonProperty("contentType") ContentType contentType,
            @JsonProperty("maxMessageSize") Integer maxMessageSize,
            @JsonProperty("auth") PublishingAuth publishingAuth,
            @JsonProperty("subscribingRestricted") boolean subscribingRestricted,
            @JsonProperty("offlineStorage") TopicDataOfflineStorage offlineStorage,
            @JsonProperty("labels") Set<TopicLabel> labels,
            @JsonProperty("createdAt") Instant createdAt,
            @JsonProperty("modifiedAt") Instant modifiedAt
    ) {
        this(TopicName.fromQualifiedName(qualifiedName), description, owner, retentionTime, migratedFromJsonType, ack,
                trackingEnabled, contentType, jsonToAvroDryRunEnabled, schemaIdAwareSerializationEnabled,
                maxMessageSize == null ? DEFAULT_MAX_MESSAGE_SIZE : maxMessageSize,
                publishingAuth == null ? PublishingAuth.disabled() : publishingAuth,
                subscribingRestricted,
                offlineStorage == null ? TopicDataOfflineStorage.defaultOfflineStorage() : offlineStorage,
                labels == null ? Collections.emptySet() : labels,
                createdAt, modifiedAt
        );
    }

    public RetentionTime getRetentionTime() {
        return retentionTime;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, description, owner, retentionTime, migratedFromJsonType, trackingEnabled, ack, contentType,
                jsonToAvroDryRunEnabled, schemaIdAwareSerializationEnabled, maxMessageSize,
                publishingAuth, subscribingRestricted, offlineStorage, labels);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final Topic other = (Topic) obj;

        return Objects.equals(this.name, other.name)
                && Objects.equals(this.description, other.description)
                && Objects.equals(this.owner, other.owner)
                && Objects.equals(this.retentionTime, other.retentionTime)
                && Objects.equals(this.jsonToAvroDryRunEnabled, other.jsonToAvroDryRunEnabled)
                && Objects.equals(this.trackingEnabled, other.trackingEnabled)
                && Objects.equals(this.migratedFromJsonType, other.migratedFromJsonType)
                && Objects.equals(this.schemaIdAwareSerializationEnabled, other.schemaIdAwareSerializationEnabled)
                && Objects.equals(this.ack, other.ack)
                && Objects.equals(this.contentType, other.contentType)
                && Objects.equals(this.maxMessageSize, other.maxMessageSize)
                && Objects.equals(this.subscribingRestricted, other.subscribingRestricted)
                && Objects.equals(this.publishingAuth, other.publishingAuth)
                && Objects.equals(this.offlineStorage, other.offlineStorage)
                && Objects.equals(this.labels, other.labels);
    }

    @JsonProperty("name")
    public String getQualifiedName() {
        return TopicName.toQualifiedName(name);
    }

    @JsonIgnore
    public TopicName getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public OwnerId getOwner() {
        return owner;
    }

    @JsonProperty("jsonToAvroDryRun")
    public boolean isJsonToAvroDryRunEnabled() {
        return jsonToAvroDryRunEnabled;
    }

    public Ack getAck() {
        return ack;
    }

    public ContentType getContentType() {
        return contentType;
    }

    public boolean isTrackingEnabled() {
        return trackingEnabled;
    }

    @JsonProperty("migratedFromJsonType")
    public boolean wasMigratedFromJsonType() {
        return migratedFromJsonType;
    }

    @JsonIgnore
    public boolean isReplicationConfirmRequired() {
        return getAck() == Ack.ALL;
    }

    public boolean isSchemaIdAwareSerializationEnabled() {
        return schemaIdAwareSerializationEnabled;
    }

    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    @JsonProperty("auth")
    public PublishingAuth getPublishingAuth() {
        return publishingAuth;
    }

    @JsonIgnore
    public boolean isAuthEnabled() {
        return publishingAuth.isEnabled();
    }

    @JsonIgnore
    public boolean isUnauthenticatedAccessEnabled() {
        return publishingAuth.isUnauthenticatedAccessEnabled();
    }

    public boolean hasPermission(String publisher) {
        return publishingAuth.hasPermission(publisher);
    }

    public boolean isSubscribingRestricted() {
        return subscribingRestricted;
    }

    public TopicDataOfflineStorage getOfflineStorage() {
        return offlineStorage;
    }

    public Set<TopicLabel> getLabels() {
        return Collections.unmodifiableSet(labels);
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Long createdAt) {
        this.createdAt = Instant.ofEpochMilli(createdAt);
    }

    public Instant getModifiedAt() {
        return modifiedAt;
    }

    public void setModifiedAt(Long modifiedAt) {
        this.modifiedAt = Instant.ofEpochMilli(modifiedAt);
    }

    @Override
    public String toString() {
        return "Topic(" + getQualifiedName() + ")";
    }

    public enum Ack {
        NONE, LEADER, ALL
    }
}
