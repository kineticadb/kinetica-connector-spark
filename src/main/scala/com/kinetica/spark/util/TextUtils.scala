package com.kinetica.spark.util

object TextUtils {

    def truncateToSize( doTruncate : Boolean, value: String, N : Integer ) : String = {
        if ( doTruncate && (value.length > N) ) {
            return value.substring(0, N);
        }
        return value;  // untruncated value
            
    }
    
}
