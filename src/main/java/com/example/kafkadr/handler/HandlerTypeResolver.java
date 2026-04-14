package com.example.kafkadr.handler;

import java.lang.reflect.Array;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.HashMap;
import java.util.Map;

/**
 * Resolves generic type parameters {@code K} and {@code V} from a
 * {@link MessageHandler MessageHandler&lt;K, V&gt;} implementation via reflection.
 *
 * <p>Supports direct implementations, abstract base classes, and anonymous classes.
 */
public final class HandlerTypeResolver {

    private HandlerTypeResolver() {
    }

    /**
     * Result holder for the two resolved type parameters.
     */
    public static class ResolvedTypes {
        private final Class<?> keyType;
        private final Class<?> valueType;

        public ResolvedTypes(Class<?> keyType, Class<?> valueType) {
            this.keyType = keyType;
            this.valueType = valueType;
        }

        public Class<?> getKeyType() {
            return keyType;
        }

        public Class<?> getValueType() {
            return valueType;
        }

        @Override
        public String toString() {
            return "ResolvedTypes{K=" + keyType.getSimpleName() + ", V=" + valueType.getSimpleName() + "}";
        }
    }

    /**
     * Extract the concrete types for K and V from {@code MessageHandler<K, V>}.
     */
    public static ResolvedTypes resolve(MessageHandler<?, ?> handler) {
        Map<TypeVariable<?>, Type> typeBindings = new HashMap<>();
        Class<?> clazz = handler.getClass();

        while (clazz != null && clazz != Object.class) {
            for (Type iface : clazz.getGenericInterfaces()) {
                ResolvedTypes result = checkType(iface, typeBindings);
                if (result != null) return result;
                collectBindings(iface, typeBindings);
            }

            Type genericSuper = clazz.getGenericSuperclass();
            if (genericSuper != null) {
                collectBindings(genericSuper, typeBindings);
            }

            clazz = clazz.getSuperclass();
        }

        throw new IllegalArgumentException(
                "Cannot resolve generic types K, V for handler: " + handler.getClass().getName() +
                ". Ensure it implements MessageHandler<K, V> with concrete types.");
    }

    private static ResolvedTypes checkType(Type type, Map<TypeVariable<?>, Type> bindings) {
        if (!(type instanceof ParameterizedType)) return null;

        ParameterizedType pt = (ParameterizedType) type;
        if (pt.getRawType() != MessageHandler.class) return null;

        Type[] args = pt.getActualTypeArguments();
        if (args.length < 2) return null;

        Class<?> keyType = resolveType(args[0], bindings);
        Class<?> valueType = resolveType(args[1], bindings);

        if (keyType == null || valueType == null) {
            throw new IllegalArgumentException(
                    "Cannot resolve MessageHandler<K, V> types: K=" + args[0] + ", V=" + args[1]);
        }

        return new ResolvedTypes(keyType, valueType);
    }

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

    private static void collectBindings(Type type, Map<TypeVariable<?>, Type> bindings) {
        if (!(type instanceof ParameterizedType)) return;

        ParameterizedType pt = (ParameterizedType) type;
        Type rawType = pt.getRawType();
        if (!(rawType instanceof Class)) return;

        TypeVariable<?>[] typeParams = ((Class<?>) rawType).getTypeParameters();
        Type[] actualArgs = pt.getActualTypeArguments();

        for (int i = 0; i < typeParams.length && i < actualArgs.length; i++) {
            Type resolved = actualArgs[i];
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
