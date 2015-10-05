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

    public JdbcFragmenter(InputData metaData) {
        super(metaData);
    }

    @Override
    public List<Fragment> getFragments() throws Exception {
        List<Fragment> fragments = new ArrayList<Fragment>();
        fragments.add(new Fragment(
                        inputData.getDataSource(),new String[]{inputData.getUserProperty("PXF_HOST")},new byte[0])
        );
        return fragments;
    }
}
