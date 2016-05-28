package org.epfl.mjoin.swift;

/**
 * Created by alex on 16.05.16.
 */

import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closeables;
import com.google.inject.Module;
import org.jclouds.Constants;
import org.jclouds.ContextBuilder;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.jclouds.openstack.swift.v1.SwiftApi;
import org.jclouds.openstack.swift.v1.SwiftApiMetadata;
import org.jclouds.openstack.swift.v1.domain.Container;
import org.jclouds.openstack.swift.v1.domain.ObjectList;
import org.jclouds.openstack.swift.v1.domain.SwiftObject;
import org.jclouds.openstack.swift.v1.features.ContainerApi;
import org.jclouds.openstack.swift.v1.features.ObjectApi;
import org.jclouds.openstack.v2_0.ServiceType;
import org.jclouds.rest.internal.BaseHttpApiMetadata;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;
import java.util.Set;

import static org.jclouds.openstack.keystone.v2_0.config.KeystoneProperties.CREDENTIAL_TYPE;
import static org.jclouds.openstack.keystone.v2_0.config.KeystoneProperties.SERVICE_TYPE;

public class TpchSwift implements Closeable {
    public static final String CONTAINER_NAME = "jclouds-example";
    public static final String OBJECT_NAME = "jclouds-example.txt";

    private SwiftApi swiftApi;


    public TpchSwift() {
        Iterable<Module> modules = ImmutableSet.<Module>of(
                new SLF4JLoggingModule());
        Properties overrides = new Properties();
        overrides.setProperty(Constants.PROPERTY_API_VERSION, "1");
        overrides.setProperty(Constants.PROPERTY_LOGGER_WIRE_LOG_SENSITIVE_INFO,"true");

        Properties properties1 = BaseHttpApiMetadata.defaultProperties();
        properties1.setProperty(SERVICE_TYPE, ServiceType.OBJECT_STORE);
        // Can alternatively be set to "tempAuthCredentials"
        properties1.setProperty(CREDENTIAL_TYPE, "tempAuthCredentials");

        String provider = "openstack-swift";
        String identity = "test1:tester1"; // tenantName:userName
        String credential = "testing1";
        SwiftApiMetadata metadata =new SwiftApiMetadata()
                .toBuilder()
                .endpointName("http://172.16.1.29:8080/auth/v1.0/")
                .version("1").defaultProperties(properties1)
                .build();

        swiftApi = ContextBuilder.newBuilder(metadata)
                .credentials(identity, credential)
                .endpoint("http://172.16.1.29:8080/auth/v1.0/")
                .modules(modules)
                .overrides(overrides)
                .buildApi(SwiftApi.class);
    }


    public void listContainers() {
        System.out.println("List Containers");
        Set<String> regions = swiftApi.getConfiguredRegions();
        for (String region : regions) {
            System.out.println("Region: " + region);
            ContainerApi containerApi = swiftApi.getContainerApi(region);
            Set<Container> containers = containerApi.list().toSet();
            System.out. println("Conatiners ->");
            for (Container container : containers) {
                System.out.println("  " + container);
                ObjectApi objectApi = swiftApi.getObjectApi(region,container.getName());
                ObjectList objectList = objectApi.list();
                System.out. println("Objects ->");
                for(SwiftObject object : objectList){
                    System.out.println("  " + object);

                }


            }
        }
    }

    public void close() throws IOException {
        Closeables.close(swiftApi, true);
    }
}
