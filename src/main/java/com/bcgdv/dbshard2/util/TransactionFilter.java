package com.bcgdv.dbshard2.util;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

import com.bcgdv.dbshard2.dao.RequestAware;
import com.bcgdv.dbshard2.dao.RequestContext;

public class TransactionFilter implements Filter {
    private static Logger logger = Logger.getLogger(TransactionFilter.class);
    
	private Map<String, RequestAware> requestAwareBeans = null;

	@Override
	public void destroy() {
	}

	@Override
	public void doFilter(ServletRequest req, ServletResponse resp,
			FilterChain chain) throws IOException, ServletException {
		RequestContext rc = new RequestContext();
		req.setAttribute(RequestContext.class.getName(), rc);
		synchronized (this) {
	        if(requestAwareBeans == null) {
	            ApplicationContext appCtx;
	            appCtx = WebApplicationContextUtils.getWebApplicationContext(req.getServletContext());
	            requestAwareBeans = appCtx.getBeansOfType(RequestAware.class);
	            for(RequestAware ra : requestAwareBeans.values()) {
	                logger.info("found RequestAware bean of " + ra.getClass());
	            }
	        }
        }
		for(RequestAware ra : requestAwareBeans.values()) {
			ra.setRequestContext(rc);
		}
		
		try {
			chain.doFilter(req, resp);
			rc.commit();
		} catch (Throwable t) {
		    if(t.getClass().getName().equals("org.apache.catalina.connector.ClientAbortException")) {
	            try {
                    System.err.println("org.apache.catalina.connector.ClientAbortException");
                    rc.commit();
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
		    }
		    else {
    			try {
    				rc.rollback();
    			} catch (SQLException e) {
    				System.err.println(e.getMessage());
    			}
    			throw new ServletException(t);
		    }
		} 
		finally {
			try {
				rc.close();
			} catch (Exception e) {
				System.err.println(e.getMessage());
			}
		}
	}

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
    }
}
