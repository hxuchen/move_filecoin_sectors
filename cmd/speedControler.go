/**
 _*_ @Author: IronHuang _*_
 _*_ @blog:https://www.dvpos.com/ _*_
 _*_ @Date: 2021/4/15 下午9:58 _*_
**/

package main

func calThreadLimit(bindWidth, singleThreadMBPS int) int {
	return bindWidth / singleThreadMBPS
}
