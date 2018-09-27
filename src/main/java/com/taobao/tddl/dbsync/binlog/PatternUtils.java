package com.taobao.tddl.dbsync.binlog;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.oro.text.regex.MalformedPatternException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.PatternCompiler;
import org.apache.oro.text.regex.Perl5Compiler;

import com.jtljia.pump.exception.ParseException;

public class PatternUtils {

    private static Map<String, Pattern> patterns = new ConcurrentHashMap<>();

    public static Pattern getPattern(String pattern) {
        if( !patterns.containsKey(pattern) ) {
        	try {
                PatternCompiler pc = new Perl5Compiler();
                patterns.put( pattern,  pc.compile(pattern, Perl5Compiler.CASE_INSENSITIVE_MASK | 
                		                                    Perl5Compiler.READ_ONLY_MASK | 
                		                                    Perl5Compiler.SINGLELINE_MASK ) );
            } catch (MalformedPatternException e) {
                throw new ParseException(e);
            }
        }
        return patterns.get( pattern );
    }

    public static void clear() {
        patterns.clear();
    }
}
