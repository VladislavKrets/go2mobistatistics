package online.omnia.go2mobi;

import com.google.gson.*;

import java.lang.reflect.Type;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by lollipop on 08.11.2017.
 */
public class StatisticsJsonDeserializer implements JsonDeserializer<List<SourceStatisticsEntity>>{
    @Override
    public List<SourceStatisticsEntity> deserialize(JsonElement jsonElement, Type type,
                                                    JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
        JsonObject object = jsonElement.getAsJsonObject();
        JsonArray array = object.get("records").getAsJsonArray();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssX");
        SourceStatisticsEntity sourceStatisticsEntity;
        List<SourceStatisticsEntity> sourceStatisticsEntities = new ArrayList<>();
        for (JsonElement element : array) {
            sourceStatisticsEntity = new SourceStatisticsEntity();
            sourceStatisticsEntity.setCampaignId(element.getAsJsonObject().get("campaign_campaign_id").getAsString());
            try {
                sourceStatisticsEntity.setDate(new Date(simpleDateFormat.parse(element.getAsJsonObject().get("date_date").getAsString()).getTime()));
            } catch (ParseException e) {
                e.printStackTrace();
            }
            sourceStatisticsEntity.setCampaignName(element.getAsJsonObject().get("campaign_name").getAsString());
            sourceStatisticsEntity.setClicks(Integer.parseInt(element.getAsJsonObject().get("click").getAsString()));
            sourceStatisticsEntity.setCtr(Double.parseDouble(element.getAsJsonObject().get("ctr").getAsString()));
            sourceStatisticsEntity.setConversions(Integer.parseInt(element.getAsJsonObject().get("conversion").getAsString()));
            sourceStatisticsEntity.setCr(Double.parseDouble(element.getAsJsonObject().get("cr").getAsString()));
            sourceStatisticsEntity.setCpm(Double.parseDouble(element.getAsJsonObject().get("cpm").getAsString()));
            sourceStatisticsEntity.setCpc(Double.parseDouble(element.getAsJsonObject().get("cpc").getAsString()));
            sourceStatisticsEntity.setSpent(Double.parseDouble(element.getAsJsonObject().get("cost").getAsString()));
            sourceStatisticsEntity.setImpressions(Integer.parseInt(element.getAsJsonObject().get("win").getAsString()));
            sourceStatisticsEntities.add(sourceStatisticsEntity);
        }
        return sourceStatisticsEntities;
    }
}
