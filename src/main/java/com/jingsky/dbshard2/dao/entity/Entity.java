package com.jingsky.dbshard2.dao.entity;

import com.fasterxml.jackson.annotation.JsonRawValue;

import java.io.Serializable;

/**
 * Created by hongweizou on 16/8/30.
 */
public class Entity implements Serializable {
    public Long id;
    public Integer version;
    public Long created;
    public Long updated;
}
