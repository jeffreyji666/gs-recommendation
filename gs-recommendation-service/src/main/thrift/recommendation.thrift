namespace java com.ctrip.gs.recommendation.thrift

enum RecommendType {
    SIGHT,     // 景点推荐
    TICKET,    // 门票推荐
    HOTEL,   // 酒店推荐
}

struct RecommendTypeParam {
    1: required i32 order , //请求次序
    2: required RecommendType recommendType, //推荐类型
    3: required i32 count = 5,   //请求条数
}

struct RecommendParam {
    1: required string url,
    2: optional string title,
    3: optional string keywords,
    4: required list<RecommendTypeParam> types,
}

struct RecommendItem {
    1: required i32 itemId,
    2: required string itemName,
    3: optional string pic,
    4: optional double score,
}

struct RecommendItemList {
    1: required RecommendTypeParam typeParam,
    2: required list<RecommendItem> items,
}

struct ClickParam {
    1: required i32 itemId,
    2: required string itemName,
    3: optional RecommendType type,
    4: optional string userid = "1",
}

service RecommendServices {
    list<RecommendItemList> recommend(1:RecommendParam param),
    void click(1:ClickParam param),
}