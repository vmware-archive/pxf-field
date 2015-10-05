package com.pivotal.pxf.plugins.jdbc;

import com.pivotal.pxf.api.Fragment;
import com.pivotal.pxf.api.Fragmenter;
import com.pivotal.pxf.api.utilities.InputData;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by gpadmin on 9/25/15.
 */
public class JdbcFragmenter extends Fragmenter {

    private String hostName;

    public JdbcFragmenter(InputData metaData) {
        super(metaData);

        Pattern p = Pattern.compile("\\/\\/.+:");
        Pattern p2 = Pattern.compile("\\/\\/.[^/:]+");

        Matcher m = p.matcher(inputData.getUserProperty("DB_URL"));
        Matcher m2 = p2.matcher(inputData.getUserProperty("DB_URL"));

        if (m.find()) {
            hostName = m.group(0).substring(2,m.group(0).length()-1);
        }else if (m2.find()) {
            hostName = m2.group(0).substring(2,m2.group(0).length());
        }

    }

    @Override
    public List<Fragment> getFragments() throws Exception {
        List<Fragment> fragments = new ArrayList<Fragment>();
        fragments.add(new Fragment(
                        inputData.getDataSource(),new String[]{hostName},new byte[0])
        );
        return fragments;
    }
}
