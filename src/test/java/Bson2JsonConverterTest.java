import java.io.*;

/*import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.undercouch.bson4jackson.BsonFactory;
import de.undercouch.bson4jackson.BsonParser;*/

import com.mongodb.util.JSON;
import operation.JBUtils;
import org.bson.*;
import org.bson.json.JsonWriter;

import org.bson.BsonDocumentReader;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


/**
 * Created by jianzchen on 2015/9/7.
 */
public class Bson2JsonConverterTest {
    public static void main(String[] args) throws Exception{
        String record = null;

        File file = new File("data/test/one_record.dat");
        InputStream in = null;
        try {
            in = new FileInputStream(file);
            int tempbyte;
            while ((tempbyte = in.read()) != -1)
                record = record + (char)tempbyte;
            in.close();
        } catch (IOException e) {e.printStackTrace();return;}

        String bsonString = record.split("\u0007",8)[7].substring(4);
        //System.out.println(bson.length());

/*        //create factory
        BsonFactory factory = new BsonFactory();

        //serialize data
        //ByteArrayOutputStream baos = new ByteArrayOutputStream();

        //deserialize data
        ByteArrayInputStream bais = new ByteArrayInputStream(bson.getBytes());

        BsonParser parser = factory.createParser(bais);
        while (parser.nextToken() != JsonToken.END_OBJECT) {
            System.out.println(parser.getCurrentName() + " : " + parser.getText());
        }*/
        //BsonDocument bsonDoc = new RawBsonDocument(bson.getBytes());
        //BsonDocumentReader bsonReader = new BsonDocumentReader(bsonDoc);
        //bsonReader.

        BasicBSONDecoder decoder = new BasicBSONDecoder();
        BSONObject bson = decoder.readObject(bsonString.getBytes());
        String json_string = JSON.serialize(bson);

        JSONParser parser = new JSONParser();

        Object obj = null;

        try
        {
            obj = parser.parse(json_string);
        }
        catch (ParseException e)
        {
            e.printStackTrace();
        }
        JSONObject json = (JSONObject) obj;
        System.out.println(json.toJSONString());


        //System.out.println(bsonString.asDocument().toJson());

    }


/*    public String json2bson(String json) {
        String bson;


        return bson;
    }*/
}
