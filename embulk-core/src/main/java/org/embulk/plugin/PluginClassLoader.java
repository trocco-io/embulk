package org.embulk.plugin;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PluginClassLoader extends URLClassLoader {
    private PluginClassLoader(
            final ClassLoader parentClassLoader,
            final URL oneNestedJarFileUrl,
            final Collection<URL> flatJarUrls,
            final Collection<String> parentFirstPackages,
            final Collection<String> parentFirstResources) {
        super(combineUrlsToArray(oneNestedJarFileUrl, flatJarUrls == null ? Collections.<URL>emptyList() : flatJarUrls),
              parentClassLoader);

        this.hasJep320LoggedWithStackTrace = false;

        this.parentFirstPackagePrefixes = ImmutableList.copyOf(
                parentFirstPackages.stream().map(pkg -> pkg + ".").collect(Collectors.toList()));
        this.parentFirstResourcePrefixes = ImmutableList.copyOf(
                parentFirstResources.stream().map(pkg -> pkg + "/").collect(Collectors.toList()));
    }

    @Deprecated  // Constructing directly with the constructor is deprecated (no warnings). Use static creator methods.
    public PluginClassLoader(
            final Collection<URL> flatJarUrls,
            final ClassLoader parentClassLoader,
            final Collection<String> parentFirstPackages,
            final Collection<String> parentFirstResources) {
        this(parentClassLoader, null, flatJarUrls, parentFirstPackages, parentFirstResources);
    }

    /**
     * Creates PluginClassLoader for plugins with dependency JARs flat on the file system, like Gem-based plugins.
     */
    public static PluginClassLoader createForFlatJars(
            final ClassLoader parentClassLoader,
            final Collection<URL> flatJarUrls,
            final Collection<String> parentFirstPackages,
            final Collection<String> parentFirstResources) {
        return new PluginClassLoader(
                parentClassLoader,
                null,
                flatJarUrls,
                parentFirstPackages,
                parentFirstResources);
    }

    /**
     * Creates PluginClassLoader for plugins with dependency JARs embedded in the plugin JAR itself, and even external.
     *
     * @param parentClassLoader  the parent ClassLoader of this PluginClassLoader instance
     * @param oneNestedJarFileUrl  "file:" URL of the plugin JAR file
     * @param dependencyJarUrls  collection of "file:" URLs of dependency JARs out of the plugin JAR
     * @param parentFirstPackages  collection of package names that are to be loaded first before the plugin's
     * @param parentFirstResources  collection of resource names that are to be loaded first before the plugin's
     */
    public static PluginClassLoader createForNestedJar(
            final ClassLoader parentClassLoader,
            final URL oneNestedJarFileUrl,
            final Collection<URL> dependencyJarUrls,
            final Collection<String> parentFirstPackages,
            final Collection<String> parentFirstResources) {
        return new PluginClassLoader(
                parentClassLoader,
                oneNestedJarFileUrl,
                dependencyJarUrls,
                parentFirstPackages,
                parentFirstResources);
    }

    /**
     * Adds the specified path to the list of URLs (for {@code URLClassLoader}) to search for classes and resources.
     *
     * It internally calls {@code URLClassLoader#addURL}.
     *
     * Some plugins (embulk-input-jdbc, for example) are calling this method to load external JAR files.
     *
     * @see <a href="https://github.com/embulk/embulk-input-jdbc/blob/ebfff0b249d507fc730c87e08b56e6aa492060ca/embulk-input-jdbc/src/main/java/org/embulk/input/jdbc/AbstractJdbcInputPlugin.java#L586-L595">embulk-input-jdbc</a>
     */
    public void addPath(Path path) {
        try {
            addUrl(path.toUri().toURL());
        } catch (MalformedURLException ex) {
            throw new IllegalArgumentException(ex);
        }
    }

    public void addUrl(URL url) {
        super.addURL(url);
    }

    /**
     * Loads the class with the specified binary name prioritized by the "parent-first" condition.
     *
     * It copy-cats {@code ClassLoader#loadClass} while the "parent-first" priorities are considered.
     *
     * If the specified class is "parent-first", it behaves the same as {@code ClassLoader#loadClass} ordered as below.
     *
     * <ol>
     *
     * <li><p>Invoke the {@code #findLoadedClass} method to check if the class has already been loaded.</p></li>
     *
     * <li><p>Invoke the parent's {@code #loadClass} method.
     *
     * <li><p>Invoke the {@code #findClass} method of this class loader to find the class.</p></li>
     *
     * </ol>
     *
     * If the specified class is "NOT parent-first", the 2nd and 3rd actions are swapped.
     *
     * @see <a href="https://docs.oracle.com/javase/7/docs/api/java/lang/ClassLoader.html#loadClass(java.lang.String,%20boolean)">Oracle Java7's ClassLoader#loadClass</a>
     * @see <a href="http://hg.openjdk.java.net/jdk7u/jdk7u/jdk/file/jdk7u141-b02/src/share/classes/java/lang/ClassLoader.java">OpenJDK7's ClassLoader</a>
     */
    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)) {
            // If the class has already been loaded by this {@code ClassLoader} or the parent's {@code ClassLoader},
            // find the loaded class and return it.
            final Class<?> loadedClass = findLoadedClass(name);

            if (loadedClass != null) {
                return resolveClass(loadedClass, resolve);
            }

            final boolean parentFirst = isParentFirstPackage(name);

            // If the class is "not parent-first" (not to be loaded by the parent at first),
            // try {@code #findClass} of the child's ({@code PluginClassLoader}'s).
            if (!parentFirst) {
                try {
                    // If a class that is removed by JEP 320 is found here, it should be fine.
                    // It means that the class is found on the plugin side -- the plugin contains its own one.
                    // Classes removed by JEP 320 are not in the "parent-first" list.
                    return resolveClass(findClass(name), resolve);
                } catch (ClassNotFoundException ignored) {
                    // Passing through intentionally.
                }
            }

            // If the class is "parent-first" (to be loaded by the parent at first), try this part at first.
            // If the class is "not parent-first" (not to be loaded by the parent at first), the above part runs first.
            try {
                final Class<?> resolvedClass = resolveClass(getParent().loadClass(name), resolve);
                logInfoIfJep320Class(name);
                return resolvedClass;
            } catch (final ClassNotFoundException ex) {
                if (!parentFirst) {
                    rethrowIfJep320Class(name, ex);
                }
                // Passing through intentionally if the class is not not removed by JEP 320.
            }

            // If the class is "parent-first" (to be loaded by the parent at first), this part runs after the above.
            if (parentFirst) {
                return resolveClass(findClass(name), resolve);
            }

            throw new ClassNotFoundException(name);
        }
    }

    private Class<?> resolveClass(Class<?> clazz, boolean resolve) {
        if (resolve) {
            resolveClass(clazz);
        }
        return clazz;
    }

    @Override
    public URL getResource(String name) {
        boolean childFirst = isParentFirstPath(name);

        if (childFirst) {
            URL childUrl = findResource(name);
            if (childUrl != null) {
                return childUrl;
            }
        }

        URL parentUrl = getParent().getResource(name);
        if (parentUrl != null) {
            return parentUrl;
        }

        if (!childFirst) {
            URL childUrl = findResource(name);
            if (childUrl != null) {
                return childUrl;
            }
        }

        return null;
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
        List<Iterator<URL>> resources = new ArrayList<>();

        boolean parentFirst = isParentFirstPath(name);

        if (!parentFirst) {
            Iterator<URL> childResources = Iterators.forEnumeration(findResources(name));
            resources.add(childResources);
        }

        Iterator<URL> parentResources = Iterators.forEnumeration(getParent().getResources(name));
        resources.add(parentResources);

        if (parentFirst) {
            Iterator<URL> childResources = Iterators.forEnumeration(findResources(name));
            resources.add(childResources);
        }

        return Iterators.asEnumeration(Iterators.concat(resources.iterator()));
    }

    private static URL[] combineUrlsToArray(final URL oneNestedJarFileUrl, final Collection<URL> flatJarUrls) {
        final int offset;
        final URL[] allDirectJarUrls;
        if (oneNestedJarFileUrl == null) {
            offset = 0;
            allDirectJarUrls = new URL[flatJarUrls.size()];
        } else {
            offset = 1;
            allDirectJarUrls = new URL[flatJarUrls.size() + 1];
            allDirectJarUrls[0] = oneNestedJarFileUrl;
        }
        int i = 0;
        for (final URL flatJarUrl : flatJarUrls) {
            allDirectJarUrls[i + offset] = flatJarUrl;
            ++i;
        }
        return allDirectJarUrls;
    }

    private boolean isParentFirstPackage(String name) {
        for (String pkg : parentFirstPackagePrefixes) {
            if (name.startsWith(pkg)) {
                return true;
            }
        }
        return false;
    }

    private boolean isParentFirstPath(String name) {
        for (String path : parentFirstResourcePrefixes) {
            if (name.startsWith(path)) {
                return true;
            }
        }
        return false;
    }

    private synchronized void logInfoIfJep320Class(final String className) {
        final int lastDotIndex = className.lastIndexOf('.');
        if (lastDotIndex != -1) {  // Found
            final String packageName = className.substring(0, lastDotIndex);
            if (JEP_320_PACKAGES.contains(packageName)) {
                if (!this.hasJep320LoggedWithStackTrace) {
                    // Logging with details and stack trace only against the first one.
                    // When a class is loaded from a library, more classes are usually loaded.
                    // It would be too noisy if details and stack trace are dumped every time.
                    logger.info(
                            "Class " + className + " is loaded by the parent ClassLoader, which is removed by JEP 320. "
                            + "The plugin needs to include it on the plugin side. "
                            + "See https://github.com/embulk/embulk/issues/1270 for more details.", new Exception());
                    this.hasJep320LoggedWithStackTrace = true;
                } else {
                    logger.info("Class " + className + " is loaded by the parent ClassLoader, which is removed by JEP 320.");
                }
            }
        }
    }

    private void rethrowIfJep320Class(final String className, final ClassNotFoundException ex) throws ClassNotFoundException {
        final int lastDotIndex = className.lastIndexOf('.');
        if (lastDotIndex != -1) {  // Found
            final String packageName = className.substring(0, lastDotIndex);
            if (JEP_320_PACKAGES.contains(packageName)) {
                throw new ClassNotFoundException(
                        "A plugin tried to load " + className + " with the parent ClassLoader. "
                        + "It is removed from JDK by JEP 320. The plugin needs to include it on the plugin side. "
                        + "See https://github.com/embulk/embulk/issues/1270 for more details.",
                        ex);
            }
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(PluginClassLoader.class);

    /**
     * Packages that are deprecated and removed since Java 11 by JEP 320.
     *
     * @see <a href="https://openjdk.java.net/jeps/320#Description">JEP 320</a>
     */
    private static String[] JEP_320_PACKAGES_ARRAY = {
        // Module java.xml.ws : JAX-WS, plus the related technologies SAAJ and Web Services Metadata
        // https://docs.oracle.com/javase/9/docs/api/java.xml.ws-summary.html
        "javax.jws",
        "javax.jws.soap",
        "javax.xml.soap",
        "javax.xml.ws",
        "javax.xml.ws.handler",
        "javax.xml.ws.handler.soap",
        "javax.xml.ws.http",
        "javax.xml.ws.soap",
        "javax.xml.ws.spi",
        "javax.xml.ws.spi.http",
        "javax.xml.ws.wsaddressing",

        // Module java.xml.bind : JAXB
        // https://docs.oracle.com/javase/9/docs/api/java.xml.bind-summary.html
        "javax.xml.bind",
        "javax.xml.bind.annotation",
        "javax.xml.bind.annotation.adapters",
        "javax.xml.bind.attachment",
        "javax.xml.bind.helpers",
        "javax.xml.bind.util",

        // Module java.activation : JAF
        // https://docs.oracle.com/javase/9/docs/api/java.activation-summary.html
        "javax.activation",

        // Module java.xml.ws.annotation : Common Annotations
        // https://docs.oracle.com/javase/9/docs/api/java.xml.ws.annotation-summary.html
        "javax.annotation",

        // Module java.corba : CORBA
        // https://docs.oracle.com/javase/9/docs/api/java.corba-summary.html
        "javax.activity",
        "javax.rmi",
        "javax.rmi.CORBA",
        "org.omg.CORBA",
        "org.omg.CORBA_2_3",
        "org.omg.CORBA_2_3.portable",
        "org.omg.CORBA.DynAnyPackage",
        "org.omg.CORBA.ORBPackage",
        "org.omg.CORBA.portable",
        "org.omg.CORBA.TypeCodePackage",
        "org.omg.CosNaming",
        "org.omg.CosNaming.NamingContextExtPackage",
        "org.omg.CosNaming.NamingContextPackage",
        "org.omg.Dynamic",
        "org.omg.DynamicAny",
        "org.omg.DynamicAny.DynAnyFactoryPackage",
        "org.omg.DynamicAny.DynAnyPackage",
        "org.omg.IOP",
        "org.omg.IOP.CodecFactoryPackage",
        "org.omg.IOP.CodecPackage",
        "org.omg.Messaging",
        "org.omg.PortableInterceptor",
        "org.omg.PortableInterceptor.ORBInitInfoPackage",
        "org.omg.PortableServer",
        "org.omg.PortableServer.CurrentPackage",
        "org.omg.PortableServer.POAManagerPackage",
        "org.omg.PortableServer.POAPackage",
        "org.omg.PortableServer.portable",
        "org.omg.PortableServer.ServantLocatorPackage",
        "org.omg.SendingContext",
        "org.omg.stub.java.rmi",

        // Module java.transaction : JTA
        // https://docs.oracle.com/javase/9/docs/api/java.transaction-summary.html
        "javax.transaction",
    };

    private static Set<String> JEP_320_PACKAGES =
            Collections.unmodifiableSet(new HashSet<String>(Arrays.asList(JEP_320_PACKAGES_ARRAY)));

    private final List<String> parentFirstPackagePrefixes;
    private final List<String> parentFirstResourcePrefixes;

    private boolean hasJep320LoggedWithStackTrace;
}
