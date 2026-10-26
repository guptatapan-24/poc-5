package com.stockstream.flink.pattern;

import com.stockstream.flink.model.PriceDowntrendEvent;
import com.stockstream.flink.model.PriceUpdate;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Flink CEP Pattern Definition for "Three consecutive price decreases within ten seconds".
 * Matches: e0 -> e1 (< e0) -> e2 (< e1) -> e3 (< e2) within 10 seconds.
 */
public class DowntrendCepPattern {

    public static Pattern<PriceUpdate, ?> createPattern() {
        // Overlap rule: skipPastLastEvent (run of 5 drops produces 1 match, not 2 overlapping ones)
        AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.skipPastLastEvent();

        return Pattern.<PriceUpdate>begin("e0", skipStrategy)
                .next("e1").where(new IterativeCondition<PriceUpdate>() {
                    @Override
                    public boolean filter(PriceUpdate current, Context<PriceUpdate> ctx) throws Exception {
                        for (PriceUpdate prev : ctx.getEventsForPattern("e0")) {
                            return current.getPrice() < prev.getPrice();
                        }
                        return false;
                    }
                })
                .next("e2").where(new IterativeCondition<PriceUpdate>() {
                    @Override
                    public boolean filter(PriceUpdate current, Context<PriceUpdate> ctx) throws Exception {
                        for (PriceUpdate prev : ctx.getEventsForPattern("e1")) {
                            return current.getPrice() < prev.getPrice();
                        }
                        return false;
                    }
                })
                .next("e3").where(new IterativeCondition<PriceUpdate>() {
                    @Override
                    public boolean filter(PriceUpdate current, Context<PriceUpdate> ctx) throws Exception {
                        for (PriceUpdate prev : ctx.getEventsForPattern("e2")) {
                            return current.getPrice() < prev.getPrice();
                        }
                        return false;
                    }
                })
                .within(Time.seconds(10));
    }

    public static PatternSelectFunction<PriceUpdate, PriceDowntrendEvent> createSelectFunction() {
        return new PatternSelectFunction<PriceUpdate, PriceDowntrendEvent>() {
            @Override
            public PriceDowntrendEvent select(Map<String, List<PriceUpdate>> pattern) {
                PriceUpdate e0 = pattern.get("e0").get(0);
                PriceUpdate e1 = pattern.get("e1").get(0);
                PriceUpdate e2 = pattern.get("e2").get(0);
                PriceUpdate e3 = pattern.get("e3").get(0);

                String eventId = "cep-" + e0.getMessageId().substring(0, Math.min(8, e0.getMessageId().length()))
                        + "-" + e3.getMessageId().substring(0, Math.min(8, e3.getMessageId().length()));

                List<Double> prices = Arrays.asList(e0.getPrice(), e1.getPrice(), e2.getPrice(), e3.getPrice());

                return new PriceDowntrendEvent(
                        eventId,
                        e0.getSymbol(),
                        prices,
                        e0.getObservedAt(),
                        e3.getObservedAt(),
                        e3.getObservedAt()
                );
            }
        };
    }
}
