package org.nerds;


import lombok.*;

import java.io.Serializable;

@Builder
@Getter
@Setter
@AllArgsConstructor
@ToString
public class TradeData implements Serializable {

    private String s; //s
    private float p; //p
    private long t; //t
    private float v; //v

}
