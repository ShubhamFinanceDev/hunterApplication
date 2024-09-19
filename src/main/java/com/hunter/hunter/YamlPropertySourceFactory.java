package com.hunter.hunter;

import java.io.IOException;
import java.util.Properties;

import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.support.EncodedResource;
import org.springframework.core.io.support.PropertySourceFactory;


//used for yml based peroperty configuration keep empty .peroperties file
public class YamlPropertySourceFactory implements PropertySourceFactory {

	@Override
	public PropertySource<?> createPropertySource(String arg0, EncodedResource arg1) throws IOException {
		// TODO Auto-generated method stub
		YamlPropertiesFactoryBean factoryBean=new YamlPropertiesFactoryBean();
		factoryBean.setResources(arg1.getResource());
		
		Properties properties=factoryBean.getObject();
		
		
		return new PropertiesPropertySource(arg1.getResource().getFilename(),properties);
	}

	
	
}
