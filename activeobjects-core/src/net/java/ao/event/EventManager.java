package net.java.ao.event;

/**
 * This is the central point of the event system where listeners are registered (and/or unregistered)
 * and events fired.
 */
public interface EventManager
{
    /**
     * <p>Published the given event.</p>
     * <p>Whether or not anyone listens to the event doesn't matter.</p>
     * @param event the event to publish
     */
    void publish(Object event);

    /**
     * Registers a listener. The definition of a correct listener is left up to the
     * implementation.
     * @param listener the listener to register.
     */
    void register(Object listener);

    /**
     * Unregisters the give listener. Nothing happens if the listener was not previously
     * registered.
     * @param listener the listener to un-register
     */
    void unregister(Object listener);
}
