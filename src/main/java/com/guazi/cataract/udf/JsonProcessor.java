package com.guazi.cataract.udf;

import java.io.Serializable;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;

import org.apache.hadoop.util.StringUtils;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

public class JsonProcessor implements Serializable {
    private static final long serialVersionUID = -7983086796740792122L;

//    private Map<String, Function> functions;

//    private Map<String, SimpleDateFormat> dateFormats;

    private JSONObject config;

    private JSONObject broadcast;

    private final static TimeZone tz = TimeZone.getTimeZone("GMT+8");

    private Map<String, List<List<String>>> columnUDF;


    public JsonProcessor(JSONObject config, JSONObject broadcast) {
        this.config = config;
        this.broadcast = broadcast;
        parseAll();
    }

    public JSONObject process(JSONObject data) {
        JSONObject out = (JSONObject) data.clone();

        for (Entry<String, Object> entry : config.entrySet()) {
            String col = entry.getKey();
            out.put(col, processCol(data, col));
        }
        return out;
    }

    public Object processCol(JSONObject data, String col) {
        List<List<String>> udfParams = columnUDF.get(col);
        List<Object> result = new ArrayList<Object>();
        for (List<String> udfParam : udfParams) {
            String funName = udfParam.get(0);

            List<Object> params = new ArrayList<Object>();
            for (int idx = 1; idx < udfParam.size(); ++idx) {
                String param = udfParam.get(idx);
                if (param.startsWith("$")) {
                    params.add(data.get(param.substring(1)));
                } else {
                    params.add(param);
                }
            }

            if (funName.equals("")) {
                result.add(params.get(0));
            } else if (funName.equalsIgnoreCase("@STR")) {
                result.add(toString(params.get(0)));
            } else if (funName.equalsIgnoreCase("@INT")) {
                result.add(toInt(params.get(0)));
            } else if (funName.equalsIgnoreCase("@RJUST")) {
                result.add(rjust(params.get(0), params.get(1)));
            } else if (funName.equalsIgnoreCase("@STRFTIME")) {
                result.add(strftime((String) params.get(0), (String) params.get(1)));
            } else if (funName.equalsIgnoreCase("@STRPTIME")) {
                result.add(strptime((String) params.get(0), (String) params.get(1)));
            } else if (funName.equalsIgnoreCase("@DIMENSION")) {
                result.add(dimension(params));
            } else if (funName.equalsIgnoreCase("@DATEFORMAT")) {
                result.add(dateFormat((String) params.get(0), (String) params.get(1), (String) params.get(2)));
            } else if (funName.equalsIgnoreCase("@IF")) {
                result.add(ifFunction(params.get(0), (String) params.get(1), (String) params.get(2), (String) params.get(3), (String) params.get(4)));
            } else if (funName.equalsIgnoreCase("@CONTAINS")) {
                result.add(isContains((String) params.get(0), (String) params.get(1)));
            } else if (funName.equalsIgnoreCase("@RANGE")) {
                result.add(range(params));
            }
        }

        if (result.isEmpty()) return null;
        if (result.get(0) instanceof String) {
            String buf = "";
            for (Object obj : result) {
                buf += (String) obj;
            }
            return buf;
        }

        if (result.get(0) instanceof Integer) {
            int buf = 0;
            for (Object obj : result) {
                buf += (int) obj;
            }
            return buf;
        }

        if (result.get(0) instanceof Float) {
            float buf = 0;
            for (Object obj : result) {
                buf += (float) obj;
            }
            return buf;
        }

        if (result.get(0) instanceof Long) {
            long buf = 0;
            for (Object obj : result) {
                buf += (long) obj;
            }
            return buf;
        }

        if (result.get(0) instanceof Double) {
            double buf = 0;
            for (Object obj : result) {
                buf += (double) obj;
            }
            return buf;
        }

        return null;
    }

    public String toString(Object obj) {
        String res = "";
        if (obj instanceof String) {
            res = (String) obj;
        } else if (obj instanceof Integer) {
            res = Integer.toString((int) obj);
        } else if (obj instanceof Float) {
            res = Float.toString((float) obj);
        } else if (obj instanceof Long) {
            res = Long.toString((long) obj);
        } else if (obj instanceof Double) {
            res = Double.toString((double) obj);
        } else if (obj instanceof Short) {
            res = Short.toString((short) obj);
        }
        return res;
    }

    public int toInt(Object obj) {
        int res = 0;
        if (obj instanceof String) {
            res = Integer.valueOf((String) obj);
        } else if (obj instanceof Integer) {
            res = (int) obj;
        }
        return res;
    }

    public void parseAll() {
        columnUDF = new HashMap<String, List<List<String>>>();
        for (Entry<String, Object> entry : config.entrySet()) {
            String col = entry.getKey();
            String cal = (String) entry.getValue();
            columnUDF.put(col, parseUDF(cal));
        }
    }

    private List<String> parseFun(String single) {
        List<String> oneFunction = new ArrayList<>();
        if (single.startsWith("@")) {
            int leftBracket = single.indexOf("(");
            int rightBracket = single.lastIndexOf(")");

            oneFunction.add(single.substring(0, leftBracket).trim());
            String params = single.substring(leftBracket + 1, rightBracket);

            for (String param : StringUtils.split(params, ',')) {
                oneFunction.add(param.trim());
            }
        } else {
            oneFunction.add("");
            oneFunction.add(single);
        }
        return oneFunction;
    }

    public List<List<String>> parseUDF(String udf) {
        List<List<String>> udfList = new ArrayList<>();
        if (udf != null && udf.contains("+")) {
            String[] udfSplit = StringUtils.split(udf, '+');
            for (String s : udfSplit) {
                udfList.add(parseFun(s.trim()));
            }
        } else {
            udfList.add(parseFun(udf));
        }
        return udfList;
    }

    public String strftime(String ts, String format) {
        SimpleDateFormat dateFormat = null;
        dateFormat = new SimpleDateFormat(format);
        dateFormat.setTimeZone(tz);

        long t = Long.parseLong(ts);
        if (ts.length() == 10) t *= 1000;

        return dateFormat.format(new Date(t));
    }


    public String strptime(String date, String dataFormat) {
        SimpleDateFormat df = null;
        df = new SimpleDateFormat(dataFormat);
        df.setTimeZone(tz);

        Date dt = null;
        try {
            dt = df.parse(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        Calendar c = Calendar.getInstance();
        c.setTime(dt);
        long time = c.getTimeInMillis();
        return Long.toString(time);
    }

    public String dateFormat(String date, String from, String to) {
        SimpleDateFormat fromDF = null;
        fromDF = new SimpleDateFormat(from);
        fromDF.setTimeZone(tz);

        SimpleDateFormat toDF = null;
        toDF = new SimpleDateFormat(to);
        toDF.setTimeZone(tz);

        Date dt = null;
        try {
            dt = fromDF.parse(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        if (dt == null) {
            System.out.println("DataFormat error: ");
            System.out.println("Date: " + date);
            System.out.println("From: " + from);
            System.out.println("To: " + to);
            return "";
        }

        return toDF.format(dt);
    }

    public Object dimension(List<Object> params) {
        String name = (String) params.get(0);
        JSONArray mappings = (JSONArray) broadcast.get(name);
        Object o = null;
        for (int i = 1; i < params.size(); ++i) {
            Object param = params.get(i);
            JSONObject mapping = (JSONObject) mappings.get(i - 1);
            o = mapping.get(param);
            if (o != null) break;
        }
        return o;
    }

    public String rjust(Object data, Object len) {
        String format = "%1$,0" + (String) len + "d";
        if (data instanceof Integer) {
            return String.format(format, (int) data);
        } else if (data instanceof String) {
            return String.format(format, Integer.parseInt((String) data));
        } else {
            System.out.println("RJUST failed, data: " + data + ", len: " + len);
        }
        return data.toString();
    }

    private Object ifFunction(Object originData, String method, String compareData, Object trueResult, Object falseResult) {
        boolean flag = false;
        if ("eq".equals(method)) {
            // ==
            if (originData instanceof String) {
                flag = originData.equals(compareData);
            } else if (originData instanceof Number) {
                NumberFormat nf = NumberFormat.getInstance();
                Number compareNumber = null;
                try {
                    compareNumber = nf.parse(compareData);
                    flag = ((Number) originData).doubleValue() == compareNumber.doubleValue();
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
        } else if ("ne".equals(method)) {
            // !=
            if (originData instanceof String) {
                flag = !originData.equals(compareData);
            } else if (originData instanceof Number) {
                NumberFormat nf = NumberFormat.getInstance();
                Number compareNumber = null;
                try {
                    compareNumber = nf.parse(compareData);
                    flag = ((Number) originData).doubleValue() != compareNumber.doubleValue();
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
        } else if ("lt".equals(method)) {
            // <
            if (originData instanceof String) {
                flag = ((String) originData).compareTo(compareData) < 0;
            } else if (originData instanceof Number) {
                NumberFormat nf = NumberFormat.getInstance();
                Number compareNumber = null;
                try {
                    compareNumber = nf.parse(compareData);
                    flag = ((Number) originData).doubleValue() < compareNumber.doubleValue();
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
        } else if ("le".equals(method)) {
            // <=
            if (originData instanceof String) {
                flag = ((String) originData).compareTo(compareData) <= 0;
            } else if (originData instanceof Number) {
                NumberFormat nf = NumberFormat.getInstance();
                Number compareNumber = null;
                try {
                    compareNumber = nf.parse(compareData);
                    flag = ((Number) originData).doubleValue() <= compareNumber.doubleValue();
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
        } else if ("gt".equals(method)) {
            // >
            if (originData instanceof String) {
                flag = ((String) originData).compareTo(compareData) > 0;
            } else if (originData instanceof Number) {
                NumberFormat nf = NumberFormat.getInstance();
                Number compareNumber = null;
                try {
                    compareNumber = nf.parse(compareData);
                    flag = ((Number) originData).doubleValue() > compareNumber.doubleValue();
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
        } else if ("ge".equals(method)) {
            // >=
            if (originData instanceof String) {
                flag = ((String) originData).compareTo(compareData) >= 0;
            } else if (originData instanceof Number) {
                NumberFormat nf = NumberFormat.getInstance();
                Number compareNumber = null;
                try {
                    compareNumber = nf.parse(compareData);
                    flag = ((Number) originData).doubleValue() >= compareNumber.doubleValue();
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
        } else if ("contains".equals(method)) {
            flag = originData.toString().contains(compareData);
        }

        if ("NULL".equals(trueResult)) {
            trueResult = null;
        }
        if ("NULL".equals(falseResult)) {
            falseResult = null;
        }
        return flag ? trueResult : falseResult;
    }

    private int isContains(String source, String str) {
        if (source != null && str != null) {
            return source.contains(str) ? 1 : 0;
        } else {
            return 0;
        }
    }

    public Object range(List<Object> params) {
        String name = (String) params.get(0);
        JSONArray mappings = (JSONArray) broadcast.get(name);
        Object o = null;
        NumberFormat nf = NumberFormat.getInstance();
        for (int i = 1; i < params.size(); ++i) {
            Object param = params.get(i);
            JSONObject mapping = (JSONObject) mappings.get(i - 1);
            for (Object obj : mapping.keySet()) {
                String key = obj.toString();
                String[] minMax = key.split("_");
                Number min = null;
                Number max = null;
                try {
                    min = nf.parse(minMax[0]);
                    max = nf.parse(minMax[1]);
                    Number input = nf.parse(param.toString());
                    if (min.doubleValue() <= input.doubleValue()
                            && max.doubleValue() >= input.doubleValue()) {
                        o = mapping.get(key);
                    }
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
            if (o != null) break;
        }
        return o;
    }

    public Map<String, List<List<String>>> getColumnUDF() {
        return columnUDF;
    }
}
