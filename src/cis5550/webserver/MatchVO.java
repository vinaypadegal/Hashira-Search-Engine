package cis5550.webserver;

import java.util.Map;

public class MatchVO {
    public Map<String, String> params;
    public Route route;
    public MatchVO(Map<String, String> params, Route route) {
        this.params = params;
        this.route = route;
    }
}
