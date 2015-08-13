package com.pivotal.pxf.plugins.dram;

public class Pair<FIRST, SECOND> {

    public FIRST first;
    public SECOND second;

    public Pair() {
    }

    public Pair(FIRST f, SECOND s) {
        this.first = f;
        this.second = s;
    }
}

