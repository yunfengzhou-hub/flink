package org.apache.flink.streaming.api.operators;

import org.apache.flink.shaded.asm9.org.objectweb.asm.ClassReader;
import org.apache.flink.shaded.asm9.org.objectweb.asm.ClassVisitor;
import org.apache.flink.shaded.asm9.org.objectweb.asm.MethodVisitor;
import org.apache.flink.shaded.asm9.org.objectweb.asm.Opcodes;

import java.util.concurrent.atomic.AtomicBoolean;

public class ExtractionUtils {
    public static boolean useTimestamp(String className) {
        final AtomicBoolean useTimestamp = new AtomicBoolean(false);

        ClassVisitor cv =new MyClassVisitor(useTimestamp);

        try {
            ClassReader classReader = new ClassReader(className);
            classReader.accept(cv, 0);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return useTimestamp.get();
    }

    private static class MyClassVisitor extends ClassVisitor {
        private final String className;
        private final AtomicBoolean useTimestamp;

        protected MyClassVisitor(AtomicBoolean useTimestamp) {
            super(Opcodes.ASM9);
            this.className = "org.apache.flink.streaming.runtime.streamrecord.StreamRecord"
                    .replace('.', '/');
            this.useTimestamp = useTimestamp;
        }

        @Override
        public MethodVisitor visitMethod(
                int access,
                String name,
                String desc,
                String signature,
                String[] exceptions) {
            return new MethodVisitor(Opcodes.ASM9) {
                @Override
                public void visitMethodInsn(
                        int opcode,
                        String owner,
                        String name,
                        String desc,
                        boolean arg4) {
                    if (owner.equals(className)
                            && (name.equals("getTimestamp")
                            || name.equals("hasTimestamp"))) {
                        String operator =
                                "--  opcode  --  "
                                        + opcode
                                        + " --  owner  --  "
                                        + owner
                                        + " --  name  --  "
                                        + name
                                        + "  --  desc  --  "
                                        + desc;
                        System.out.println(operator);
                        useTimestamp.set(true);
                    }
                    super.visitMethodInsn(opcode, owner, name, desc, arg4);
                }
            };
        }
    }
}
