/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net

import (
	"strconv"
)

type EndPointID = int32
type EndPointType int32

const (
	TCP_CLIENT EndPointType = 2
	TCP_SERVER EndPointType = 7
)

var EndPointType_name = map[int32]string{
	2: "TCP_CLIENT",
	7: "TCP_SERVER",
}

var EndPointType_value = map[string]int32{
	"TCP_CLIENT": 2,
	"TCP_SERVER": 7,
}

func (x EndPointType) String() string {
	s, ok := EndPointType_name[int32(x)]
	if ok {
		return s
	}

	return strconv.Itoa(int(x))
}
