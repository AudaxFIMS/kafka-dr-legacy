package com.example.kafkadr.handler;

import java.lang.reflect.Array;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.HashMap;
import java.util.Map;

/**
 * Resolves the generic type parameter {@code T} from a {@link MessageHandler}
 * implementation via reflection.
 *
 * <p>Supports:
 * <ul>
 *   <li>Direct implementation: {@code class X implements MessageHandler<String>}</li>
 *   <li>Abstract base class: {@code class X extends Base<String>} where
 *       {@code Base<T> implements MessageHandler<T>}</li>
 *   <li>Anonymous classes</li>
 * </ul>
 */
public final class HandlerTypeResolver {

    private HandlerTypeResolver() {
    }

    /**
     * Extract the concrete {@code Class<?>} for the type parameter T
     * of {@code MessageHandler<T>} from the given handler instance.
     */
    public static Class<?> resolve(MessageHandler<?> handler) {
        Map<TypeVariable<?>, Type> typeBindings = new HashMap<>();
        Class<?> clazz = handler.getClass();

        // Walk up the class hierarchy, collecting type variable bindings
        while (clazz != null && clazz != Object.class) {
            // Check interfaces at this level
            Type[] genericInterfaces = clazz.getGenericInterfaces();
            for (Type iface : genericInterfaces) {
                Class<?> result = checkType(iface, typeBindings);
                if (result != null) return result;

                // If the interface is parameterized, record its type bindings
                collectBindings(iface, typeBindings);
            }

            // Record bindings from the superclass declaration
            Type genericSuper = clazz.getGenericSuperclass();
            if (genericSuper != null) {
                collectBindings(genericSuper, typeBindings);
            }

            clazz = clazz.getSuperclass();
        }

        throw new IllegalArgumentException(
                "Cannot resolve generic type T for handler: " + handler.getClass().getName() +
                ". Ensure it implements MessageHandler<T> with a concrete type.");
    }

    /**
     * If {@code type} is {@code MessageHandler<X>}, resolve X to a Class and return it.
     */
    private static Class<?> checkType(Type type, Map<TypeVariable<?>, Type> bindings) {
        if (!(type instanceof ParameterizedType)) return null;

        ParameterizedType pt = (ParameterizedType) type;
        if (pt.getRawType() != MessageHandler.class) return null;

        Type arg = pt.getActualTypeArguments()[0];
        return resolveType(arg, bindings);
    }

    /**
     * Resolve a Type to a concrete Class, following TypeVariable bindings if needed.
     */
    private static Class<?> resolveType(Type type, Map<TypeVariable<?>, Type> bindings) {
        if (type instanceof Class) {
            return (Class<?>) type;
        }
        if (type instanceof TypeVariable) {
            Type bound = bindings.get(type);
            if (bound != null) {
                return resolveType(bound, bindings);
            }
            return null;
        }
        if (type instanceof ParameterizedType) {
            Type raw = ((ParameterizedType) type).getRawType();
            if (raw instanceof Class) return (Class<?>) raw;
        }
        if (type instanceof GenericArrayType) {
            Type component = ((GenericArrayType) type).getGenericComponentType();
            Class<?> componentClass = resolveType(component, bindings);
            if (componentClass != null) {
                return Array.newInstance(componentClass, 0).getClass();
            }
        }
        return null;
    }

    /**
     * Record mappings from TypeVariables to their actual type arguments.
     * E.g., for {@code class Foo extends Base<String>}, if Base declares {@code <T>},
     * we record {@code T → String}.
     */
    private static void collectBindings(Type type, Map<TypeVariable<?>, Type> bindings) {
        if (!(type instanceof ParameterizedType)) return;

        ParameterizedType pt = (ParameterizedType) type;
        Type rawType = pt.getRawType();
        if (!(rawType instanceof Class)) return;

        TypeVariable<?>[] typeParams = ((Class<?>) rawType).getTypeParameters();
        Type[] actualArgs = pt.getActualTypeArguments();

        for (int i = 0; i < typeParams.length && i < actualArgs.length; i++) {
            Type resolved = actualArgs[i];
            // If the actual arg is itself a type variable, follow existing bindings
            if (resolved instanceof TypeVariable) {
                Type bound = bindings.get(resolved);
                if (bound != null) {
                    resolved = bound;
                }
            }
            bindings.put(typeParams[i], resolved);
        }
    }
}
