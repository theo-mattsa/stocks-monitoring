package com.soe.domain.events;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME, 
    include = JsonTypeInfo.As.EXISTING_PROPERTY, 
    property = "type",
    visible = true
)
@JsonSubTypes({
    @JsonSubTypes.Type(value = PriceSpikeEvent.class, name = "WINDOW_PRICE_SPIKE"),
    @JsonSubTypes.Type(value = ReachedDayHighEvent.class, name = "DAY_HIGH_REACHED"),
    @JsonSubTypes.Type(value = ReachedDayLowEvent.class, name = "DAY_LOW_REACHED"),
    @JsonSubTypes.Type(value = SignificantPriceChangeEvent.class, name = "SIGNIFICANT_PRICE_CHANGE")
})
@JsonIgnoreProperties (ignoreUnknown = true)
public interface MarketEvent {
    String getType();
    String getSymbol();
    String constructMessage();
}