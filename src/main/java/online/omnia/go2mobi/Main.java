package online.omnia.go2mobi;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by lollipop on 08.11.2017.
 */
public class Main {
    public static int days;
    public static long deltaTime = 24 * 60 * 60 * 1000;

    public static void main(String[] args){
        try {
            if (args.length != 1) {
                return;
            }
            if (!args[0].matches("\\d+")) return;
            if (Integer.parseInt(args[0]) == 0) {
                deltaTime = 0;
            }
            days = Integer.parseInt(args[0]);

            List<AccountsEntity> accountsEntities = MySQLDaoImpl.getInstance().getAccountsEntities("Go2Mobi");
            HttpURLConnection httpcon;

            String answer;
            Date currentDate = new Date();
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            String str = "{\"groupings\":[" +

                    "\"campaign_campaign_id\"," +
                    "\"date_date\"," +
                    "\"campaign_name\"" +
                    "]," +
                    "\"columns\":[" +
                    "\"click\"," +
                    "\"ctr\"," +
                    "\"conversion\"," +
                    "\"cr\"," +
                    "\"cpm\"," +
                    "\"cpc\"," +
                    "\"cpa\"," +
                    "\"cost\"," +
                    "\"revenue\"," +
                    "\"profit\"" +
                    "]," +
                    "\"filters\":[" +
                    "]," +
                    "\"start_date\":\"" + simpleDateFormat.format(new Date(currentDate.getTime() - deltaTime - days * 24L * 60 * 60 * 1000)) + "\"," +
                    "\"end_date\":\"" + simpleDateFormat.format(new Date(currentDate.getTime() - deltaTime)) + "\"" +
                    "}";
            System.out.println(str);
            GsonBuilder builder = new GsonBuilder();
            builder.registerTypeAdapter(List.class, new StatisticsJsonDeserializer());
            builder.registerTypeAdapter(String.class, new JsonUrlDeserializer());
            Gson gson = builder.create();
            BufferedReader reader;
            OutputStream os;
            List<SourceStatisticsEntity> sourceStatisticsEntities;
            SourceStatisticsEntity entity;
            String url;
            Map<String, String> parameters;
            for (AccountsEntity accountsEntity : accountsEntities) {
                httpcon = (HttpURLConnection) ((new URL("https://api.go2mobi.com/v1/reports").openConnection()));
                httpcon.setDoOutput(true);
                httpcon.setRequestProperty("Content-Type", "application/json");
                httpcon.setRequestProperty("Authorization", "Token " + accountsEntity.getApiKey());
                httpcon.setRequestMethod("POST");
                httpcon.connect();
                if (accountsEntity.getActual() == 1) {
                    byte[] outputBytes = str.getBytes("UTF-8");
                    os = httpcon.getOutputStream();
                    os.write(outputBytes);
                    os.close();
                    reader = new BufferedReader(new InputStreamReader(httpcon.getInputStream()));
                    String line;
                    StringBuilder lineBuilder = new StringBuilder();
                    while ((line = reader.readLine()) != null) {
                        lineBuilder.append(line);
                    }
                    try {
                        answer = HttpMethodUtils.getMethod(httpcon.getHeaderField("Location") + "?page_number=1&page_size=500", accountsEntity.getApiKey());
                        sourceStatisticsEntities = gson.fromJson(answer, List.class);
                    } catch (Exception e) {
                        Utils.writeLog(e.toString());
                        continue;
                    }
                    for (SourceStatisticsEntity sourceStatisticsEntity : sourceStatisticsEntities) {
                        sourceStatisticsEntity.setAccount_id(accountsEntity.getAccountId());
                        sourceStatisticsEntity.setBuyerId(accountsEntity.getBuyerId());
                        sourceStatisticsEntity.setReceiver("API");

                        try {
                            answer = HttpMethodUtils.getMethod("https://api.go2mobi.com/v1/campaigns/" + sourceStatisticsEntity.getCampaignId(), accountsEntity.getApiKey());
                            url = gson.fromJson(answer, String.class);
                        } catch (Exception e) {
                            Utils.writeLog(e.toString());
                            continue;
                        }
                        parameters = Utils.getUrlParameters(url);
                        if (parameters.containsKey("cab")) {
                            if (parameters.get("cab").matches("\\d+")
                                    && MySQLDaoImpl.getInstance().getAffiliateByAfid(Integer.parseInt(parameters.get("cab"))) != null) {
                                sourceStatisticsEntity.setAfid(Integer.parseInt(parameters.get("cab")));
                            } else {
                                sourceStatisticsEntity.setAfid(0);
                            }
                        } else sourceStatisticsEntity.setAfid(2);
                        if (Main.days != 0) {
                            entity = MySQLDaoImpl.getInstance().getSourceStatistics(sourceStatisticsEntity.getAccount_id(),
                                    sourceStatisticsEntity.getDate());
                            if (entity != null) {
                                sourceStatisticsEntity.setId(entity.getId());
                                MySQLDaoImpl.getInstance().updateSourceStatistics(sourceStatisticsEntity);
                                entity = null;
                            } else MySQLDaoImpl.getInstance().addSourceStatistics(sourceStatisticsEntity);
                        }
                        else {
                            if (MySQLDaoImpl.getInstance().isDateInTodayAdsets(sourceStatisticsEntity.getDate(), sourceStatisticsEntity.getAccount_id(), sourceStatisticsEntity.getCampaignId())) {
                                MySQLDaoImpl.getInstance().updateTodayAdset(Utils.getAdset(sourceStatisticsEntity));
                            } else MySQLDaoImpl.getInstance().addTodayAdset(Utils.getAdset(sourceStatisticsEntity));

                        }
                    }
                }
            }
        } catch (NumberFormatException | JsonSyntaxException | IOException e) {
            e.printStackTrace();
        }
        MySQLDaoImpl.getSessionFactory().close();
    }
}
