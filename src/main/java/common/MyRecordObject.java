package common;

/**
 * Created by cjz20 on 2015/12/21.
 */
public class MyRecordObject {

    private Long id;
    private String name;
    private String description;

    public MyRecordObject(String record) {
        String[] recordTmp = record.split("\t");
        this.id = Long.parseLong(recordTmp[0]);
        this.name = recordTmp[1];
        this.description = recordTmp[2];
    }

    public Long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }
}
